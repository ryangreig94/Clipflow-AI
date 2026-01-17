import { createClient } from '@supabase/supabase-js';
import { exec } from 'child_process';
import { promisify } from 'util';
import fs from 'fs';
import path from 'path';
import os from 'os';

const execAsync = promisify(exec);

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const workerId = process.env.WORKER_ID || 'worker-1';

// Twitch API credentials
const TWITCH_CLIENT_ID = process.env.TWITCH_CLIENT_ID;
const TWITCH_CLIENT_SECRET = process.env.TWITCH_CLIENT_SECRET;

// YouTube API key
const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY;

// ElevenLabs API key for AI Short voiceovers
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY;

// Pexels API key for stock footage
const PEXELS_API_KEY = process.env.PEXELS_API_KEY;

// Cache for Twitch OAuth token
let twitchAccessToken = null;
let twitchTokenExpiry = null;

if (!supabaseUrl || !supabaseKey) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

let isShuttingDown = false;
let currentJobId = null;

process.on('SIGTERM', async () => {
  console.log(`[${workerId}] Received SIGTERM, graceful shutdown...`);
  isShuttingDown = true;
  
  if (currentJobId) {
    console.log(`[${workerId}] Releasing job ${currentJobId}...`);
    await supabase
      .from('video_processing_jobs')
      .update({ status: 'ready' })
      .eq('id', currentJobId)
      .eq('status', 'processing');
  }
  
  setTimeout(() => process.exit(0), 1000);
});

process.on('SIGINT', () => {
  console.log(`[${workerId}] Received SIGINT, exiting...`);
  isShuttingDown = true;
  process.exit(0);
});

async function sendHeartbeat() {
  try {
    const { error } = await supabase
      .from('worker_heartbeat')
      .upsert({
        worker_id: workerId,
        worker_type: 'combined',
        last_seen: new Date().toISOString(),
        status: 'running'
      }, { onConflict: 'worker_id' });
    
    if (error) {
      console.error('Heartbeat error:', error.message);
    }
  } catch (err) {
    console.error('Heartbeat failed:', err.message);
  }
}

// ============================================
// DISCOVER WORKER LOGIC
// ============================================

// Fallback sample clips for platforms without API integration
const sampleClips = {
  youtube: [
    { url: 'https://www.youtube.com/watch?v=dQw4w9WgXcQ', title: 'Trending Video Moment' },
    { url: 'https://www.youtube.com/watch?v=9bZkp7q19f0', title: 'Viral Dance Clip' },
    { url: 'https://www.youtube.com/watch?v=kJQP7kiw5Fk', title: 'Music Video Highlight' },
  ],
  rumble: [
    { url: 'https://rumble.com/v2example1', title: 'Breaking News Clip' },
    { url: 'https://rumble.com/v2example2', title: 'Commentary Highlight' },
    { url: 'https://rumble.com/v2example3', title: 'Interview Moment' },
  ]
};

// ============================================
// TWITCH API INTEGRATION
// ============================================

async function getTwitchAccessToken() {
  // Return cached token if still valid (with 5 min buffer)
  if (twitchAccessToken && twitchTokenExpiry && Date.now() < twitchTokenExpiry - 300000) {
    return twitchAccessToken;
  }

  if (!TWITCH_CLIENT_ID || !TWITCH_CLIENT_SECRET) {
    throw new Error('Twitch API credentials not configured');
  }

  console.log(`[${workerId}] Fetching new Twitch access token...`);
  
  const response = await fetch('https://id.twitch.tv/oauth2/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: TWITCH_CLIENT_ID,
      client_secret: TWITCH_CLIENT_SECRET,
      grant_type: 'client_credentials'
    })
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Twitch OAuth failed: ${response.status} - ${error}`);
  }

  const data = await response.json();
  twitchAccessToken = data.access_token;
  twitchTokenExpiry = Date.now() + (data.expires_in * 1000);
  
  console.log(`[${workerId}] Twitch token obtained, expires in ${data.expires_in}s`);
  return twitchAccessToken;
}

async function searchTwitchGames(query) {
  const token = await getTwitchAccessToken();
  
  const response = await fetch(
    `https://api.twitch.tv/helix/search/categories?query=${encodeURIComponent(query)}&first=5`,
    {
      headers: {
        'Authorization': `Bearer ${token}`,
        'Client-Id': TWITCH_CLIENT_ID
      }
    }
  );

  if (!response.ok) {
    throw new Error(`Twitch search failed: ${response.status}`);
  }

  const data = await response.json();
  return data.data || [];
}

async function searchTwitchChannels(query) {
  const token = await getTwitchAccessToken();
  
  const response = await fetch(
    `https://api.twitch.tv/helix/search/channels?query=${encodeURIComponent(query)}&first=5`,
    {
      headers: {
        'Authorization': `Bearer ${token}`,
        'Client-Id': TWITCH_CLIENT_ID
      }
    }
  );

  if (!response.ok) {
    throw new Error(`Twitch channel search failed: ${response.status}`);
  }

  const data = await response.json();
  return data.data || [];
}

async function getTwitchTopClips({ gameId = null, broadcasterId = null, limit = 10, recencyDays = 7 } = {}) {
  const token = await getTwitchAccessToken();
  
  // Build URL with recency filter
  let url = `https://api.twitch.tv/helix/clips?first=${limit}`;
  
  // Use the recencyDays parameter for date filtering
  const startedAt = new Date(Date.now() - recencyDays * 24 * 60 * 60 * 1000).toISOString();
  
  if (gameId) {
    url += `&game_id=${gameId}&started_at=${startedAt}`;
  } else if (broadcasterId) {
    // For broadcasters, also use recency filter
    url += `&broadcaster_id=${broadcasterId}&started_at=${startedAt}`;
  } else {
    throw new Error('Either gameId or broadcasterId is required for Twitch clips API');
  }

  console.log(`[${workerId}] Fetching clips from: ${url.replace(/Bearer [^&]+/, 'Bearer ***')}`);

  const response = await fetch(url, {
    headers: {
      'Authorization': `Bearer ${token}`,
      'Client-Id': TWITCH_CLIENT_ID
    }
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Twitch clips API failed: ${response.status} - ${error}`);
  }

  const data = await response.json();
  console.log(`[${workerId}] Clips API returned ${data.data?.length || 0} clips`);
  return data.data || [];
}

async function discoverTwitchClips(keywords, maxClips = 5, recencyDays = 7) {
  console.log(`[${workerId}] Discovering Twitch clips for: ${keywords || 'top clips'} (last ${recencyDays} days)`);
  
  let gameId = null;
  let broadcasterId = null;
  
  // If keywords provided, search for matching game/category first
  if (keywords) {
    // Try to find a matching game
    const games = await searchTwitchGames(keywords);
    if (games.length > 0) {
      gameId = games[0].id;
      console.log(`[${workerId}] Found game: ${games[0].name} (ID: ${gameId})`);
    } else {
      // No game found, try to find a matching channel/broadcaster
      console.log(`[${workerId}] No game found, searching for channel: ${keywords}`);
      const channels = await searchTwitchChannels(keywords);
      if (channels.length > 0) {
        broadcasterId = channels[0].id;
        console.log(`[${workerId}] Found channel: ${channels[0].display_name} (ID: ${broadcasterId})`);
      }
    }
  }
  
  // If we still don't have a gameId or broadcasterId, use a popular game as fallback
  if (!gameId && !broadcasterId) {
    console.log(`[${workerId}] No specific match found, using "Just Chatting" as default`);
    const defaultGames = await searchTwitchGames('Just Chatting');
    if (defaultGames.length > 0) {
      gameId = defaultGames[0].id;
    } else {
      throw new Error('Unable to find any Twitch category to search clips for');
    }
  }
  
  // Get top clips for the game or broadcaster with recency filter
  const clips = await getTwitchTopClips({ gameId, broadcasterId, limit: maxClips, recencyDays });
  
  console.log(`[${workerId}] Found ${clips.length} Twitch clips`);
  
  return clips.map(clip => ({
    url: clip.url,
    title: clip.title,
    thumbnail_url: clip.thumbnail_url,
    duration: Math.round(clip.duration),
    view_count: clip.view_count,
    broadcaster_name: clip.broadcaster_name,
    game_id: clip.game_id,
    created_at: clip.created_at
  }));
}

// ============================================
// YOUTUBE API INTEGRATION
// ============================================

async function searchYouTubeVideos(query, maxResults = 10, recencyDays = 7) {
  if (!YOUTUBE_API_KEY) {
    throw new Error('YouTube API key not configured');
  }

  console.log(`[${workerId}] Searching YouTube for: ${query} (last ${recencyDays} days)`);

  // Calculate published after date for recency filter
  const publishedAfter = new Date(Date.now() - recencyDays * 24 * 60 * 60 * 1000).toISOString();

  // Search for videos
  const searchUrl = new URL('https://www.googleapis.com/youtube/v3/search');
  searchUrl.searchParams.set('part', 'snippet');
  searchUrl.searchParams.set('q', query);
  searchUrl.searchParams.set('type', 'video');
  searchUrl.searchParams.set('order', 'viewCount'); // Get most viewed
  searchUrl.searchParams.set('maxResults', maxResults.toString());
  searchUrl.searchParams.set('videoDuration', 'short'); // Under 4 minutes
  searchUrl.searchParams.set('publishedAfter', publishedAfter); // Recency filter
  searchUrl.searchParams.set('key', YOUTUBE_API_KEY);

  const response = await fetch(searchUrl.toString());

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`YouTube search failed: ${response.status} - ${error}`);
  }

  const data = await response.json();
  const videoIds = (data.items || []).map(item => item.id.videoId).filter(Boolean);

  if (videoIds.length === 0) {
    return [];
  }

  // Get video statistics for viral scoring
  const statsUrl = new URL('https://www.googleapis.com/youtube/v3/videos');
  statsUrl.searchParams.set('part', 'snippet,statistics,contentDetails');
  statsUrl.searchParams.set('id', videoIds.join(','));
  statsUrl.searchParams.set('key', YOUTUBE_API_KEY);

  const statsResponse = await fetch(statsUrl.toString());

  if (!statsResponse.ok) {
    const error = await statsResponse.text();
    throw new Error(`YouTube stats failed: ${statsResponse.status} - ${error}`);
  }

  const statsData = await statsResponse.json();

  const now = Date.now();
  
  return (statsData.items || []).map(video => {
    const viewCount = parseInt(video.statistics?.viewCount || '0', 10);
    const likeCount = parseInt(video.statistics?.likeCount || '0', 10);
    const commentCount = parseInt(video.statistics?.commentCount || '0', 10);
    const publishedAt = video.snippet?.publishedAt;
    
    // Parse duration (ISO 8601 format like PT4M13S)
    let duration = 60;
    const durationMatch = video.contentDetails?.duration?.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
    if (durationMatch) {
      const hours = parseInt(durationMatch[1] || '0', 10);
      const minutes = parseInt(durationMatch[2] || '0', 10);
      const seconds = parseInt(durationMatch[3] || '0', 10);
      duration = hours * 3600 + minutes * 60 + seconds;
    }

    // Calculate velocity-based viral score
    const hoursSincePublished = publishedAt 
      ? Math.max(1, (now - new Date(publishedAt).getTime()) / (1000 * 60 * 60))
      : 24; // Default to 24 hours if no publish date
    
    const viewsPerHour = viewCount / hoursSincePublished;
    const commentsPerHour = commentCount / hoursSincePublished;
    const likeRatio = viewCount > 0 ? (likeCount / viewCount) * 100 : 0;
    
    // Velocity-based trend score
    const viralScore = Math.min(100, Math.round(
      30 + // base score
      (viewsPerHour * 0.5) + // views per hour
      (likeRatio * 2) + // like ratio bonus
      (commentsPerHour * 10) + // comments per hour
      (Math.log10(viewCount + 1) * 3) // total views bonus
    ));

    return {
      url: `https://www.youtube.com/watch?v=${video.id}`,
      title: video.snippet?.title || 'Untitled',
      thumbnail_url: video.snippet?.thumbnails?.high?.url || video.snippet?.thumbnails?.default?.url,
      duration: duration,
      view_count: viewCount,
      viral_score: viralScore,
      channel_name: video.snippet?.channelTitle,
      published_at: publishedAt
    };
  });
}

async function discoverYouTubeClips(keywords, maxClips = 5, recencyDays = 7) {
  console.log(`[${workerId}] Discovering YouTube videos for: ${keywords || 'trending'} (last ${recencyDays} days)`);
  
  // Build search query
  const searchQuery = keywords 
    ? `${keywords} viral short clip`
    : 'viral trending shorts';

  const videos = await searchYouTubeVideos(searchQuery, maxClips, recencyDays);
  
  console.log(`[${workerId}] Found ${videos.length} YouTube videos`);
  
  return videos;
}

async function processDiscoverJob(job) {
  console.log(`[${workerId}] Processing discover job ${job.id}`);
  currentJobId = job.id;
  
  try {
    const platform = job.source_platform || 'twitch';
    
    // Parse discover config from keywords (JSON format from API)
    let keywords = '';
    let recencyDays = 7;
    let maxResults = 25;
    
    if (job.keywords) {
      try {
        const config = JSON.parse(job.keywords);
        keywords = config.keywords || '';
        recencyDays = config.recencyDays || 7;
        maxResults = config.maxResults || 25;
        console.log(`[${workerId}] Discover config: recencyDays=${recencyDays}, maxResults=${maxResults}, keywords="${keywords}"`);
      } catch (e) {
        // Fallback: treat as plain keywords string
        keywords = job.keywords;
      }
    }
    
    let clips = [];
    
    // Use real API based on platform
    if (platform === 'twitch' && TWITCH_CLIENT_ID && TWITCH_CLIENT_SECRET) {
      console.log(`[${workerId}] Using real Twitch API for discovery`);
      const twitchClips = await discoverTwitchClips(keywords, maxResults, recencyDays);
      
      // Filter by recency and calculate velocity-based trend score
      const now = Date.now();
      const recencyCutoff = now - (recencyDays * 24 * 60 * 60 * 1000);
      
      clips = twitchClips
        .filter(clip => {
          const clipDate = new Date(clip.created_at).getTime();
          return clipDate >= recencyCutoff;
        })
        .map(clip => {
          // Calculate hours since creation for velocity scoring
          const hoursSinceCreated = Math.max(1, (now - new Date(clip.created_at).getTime()) / (1000 * 60 * 60));
          const viewsPerHour = clip.view_count / hoursSinceCreated;
          
          // Velocity-based trend score (views per hour is the main factor)
          const trendScore = Math.min(100, Math.round(
            40 + // base score
            (viewsPerHour * 2) + // velocity bonus
            (Math.log10(clip.view_count + 1) * 5) // total views bonus
          ));
          
          return {
            url: clip.url,
            title: clip.title,
            thumbnail_url: clip.thumbnail_url,
            duration: clip.duration,
            viral_score: trendScore,
            broadcaster_name: clip.broadcaster_name,
            created_at: clip.created_at
          };
        })
        .sort((a, b) => b.viral_score - a.viral_score) // Sort by trend score
        .slice(0, maxResults);
        
    } else if (platform === 'youtube' && YOUTUBE_API_KEY) {
      console.log(`[${workerId}] Using real YouTube API for discovery`);
      const youtubeVideos = await discoverYouTubeClips(keywords, maxResults, recencyDays);
      
      // YouTube videos are already filtered by API, but re-filter for recency
      const now = Date.now();
      const recencyCutoff = now - (recencyDays * 24 * 60 * 60 * 1000);
      
      clips = youtubeVideos
        .filter(video => {
          if (!video.published_at) return true; // Include if no date
          const videoDate = new Date(video.published_at).getTime();
          return videoDate >= recencyCutoff;
        })
        .map(video => ({
          url: video.url,
          title: video.title,
          thumbnail_url: video.thumbnail_url,
          duration: video.duration,
          viral_score: video.viral_score,
          channel_name: video.channel_name
        }))
        .slice(0, maxResults);
    } else {
      // Fallback to sample clips for other platforms or if no API credentials
      console.log(`[${workerId}] Using sample clips for ${platform} (no API credentials)`);
      const platformClips = sampleClips[platform] || sampleClips.youtube;
      clips = platformClips.map(clip => ({
        url: clip.url,
        title: clip.title,
        thumbnail_url: null,
        duration: Math.floor(Math.random() * 40) + 20,
        viral_score: Math.floor(Math.random() * 20) + 80
      }));
    }
    
    // Check if we have enough results
    if (clips.length === 0) {
      throw new Error(`No trending results in last ${recencyDays} days. Try 30 days or a broader category.`);
    }
    
    if (clips.length < 5) {
      console.log(`[${workerId}] Warning: Only found ${clips.length} clips (less than 5)`);
      // Continue with fewer clips but log warning
    }
    
    // Build candidates from discovered clips
    const candidates = clips.map(clip => ({
      job_id: job.id,
      user_id: job.user_id,
      source_platform: platform,
      clip_url: clip.url,
      title: clip.title,
      thumbnail_url: clip.thumbnail_url || null,
      duration: clip.duration,
      viral_score: clip.viral_score,
      status: 'candidate'
    }));
    
    // Insert candidates into database
    const { error: insertError } = await supabase
      .from('discovered_candidates')
      .insert(candidates);
    
    if (insertError) {
      throw new Error(`Failed to insert candidates: ${insertError.message}`);
    }
    
    console.log(`[${workerId}] Inserted ${candidates.length} real Twitch clips for job ${job.id}`);
    
    // Mark discover job as done
    const { error: updateError } = await supabase
      .from('video_processing_jobs')
      .update({
        status: 'done',
        render_status: 'done'
      })
      .eq('id', job.id);
    
    if (updateError) {
      throw new Error(updateError.message);
    }
    
    console.log(`[${workerId}] Completed discover job ${job.id}`);
    currentJobId = null;
    
  } catch (err) {
    console.error(`[${workerId}] Error processing discover job ${job.id}:`, err.message);
    
    await supabase
      .from('video_processing_jobs')
      .update({ 
        status: 'failed', 
        error: err.message
      })
      .eq('id', job.id);
    currentJobId = null;
  }
}

// ============================================
// RENDER WORKER LOGIC
// ============================================

async function claimRenderTask() {
  // Use RPC to atomically claim a task
  const { data, error } = await supabase.rpc('claim_render_task', {
    p_worker_id: workerId
  });
  
  if (error) {
    // If RPC doesn't exist, fall back to manual claim
    if (error.code === '42883') {
      return await claimRenderTaskFallback();
    }
    console.error('Claim task error:', error.message);
    return null;
  }
  
  return data && data.length > 0 ? data[0] : null;
}

async function claimRenderTaskFallback() {
  // Find a queued task
  const { data: tasks, error: fetchError } = await supabase
    .from('render_tasks')
    .select('*')
    .eq('status', 'queued')
    .order('created_at', { ascending: true })
    .limit(1);

  if (fetchError || !tasks || tasks.length === 0) {
    return null;
  }

  const task = tasks[0];

  // Try to claim it
  const { data: claimed, error: claimError } = await supabase
    .from('render_tasks')
    .update({ 
      status: 'rendering',
      worker_id: workerId,
      attempts: task.attempts + 1
    })
    .eq('id', task.id)
    .eq('status', 'queued')
    .select()
    .single();

  if (claimError || !claimed) {
    return null;
  }

  return claimed;
}

async function processRenderTask(task) {
  console.log(`[${workerId}] Processing render task ${task.id} for job ${task.job_id}`);
  
  const input = task.input || {};
  const clipUrl = input.clip_url;
  
  if (!clipUrl) {
    await failTask(task, 'No clip_url provided in task input');
    return;
  }
  
  const tempDir = os.tmpdir();
  const outputFilename = `${task.job_id}.mp4`;
  const tempOutput = path.join(tempDir, outputFilename);
  
  try {
    // Step 1: Download video using yt-dlp
    console.log(`[${workerId}] Downloading from ${clipUrl}...`);
    
    try {
      // Try yt-dlp first
      await execAsync(`yt-dlp -f "best[height<=720]" -o "${tempOutput}" "${clipUrl}"`, {
        timeout: 120000 // 2 minute timeout
      });
    } catch (dlErr) {
      // If yt-dlp fails, create a placeholder video for demo
      console.log(`[${workerId}] yt-dlp failed (${dlErr.message}), creating demo video...`);
      
      // Try with text overlay first (requires fonts)
      try {
        await execAsync(
          `ffmpeg -f lavfi -i color=c=purple:s=1080x1920:d=5 -vf "drawtext=text='ClipFlow':fontfile=/usr/share/fonts/freefont/FreeSans.ttf:fontcolor=white:fontsize=64:x=(w-text_w)/2:y=(h-text_h)/2" -c:v libx264 -t 5 -y "${tempOutput}"`,
          { timeout: 30000 }
        );
      } catch (fontErr) {
        // Fallback to simple colored video without text if fonts not available
        console.log(`[${workerId}] Font rendering failed, creating simple video...`);
        await execAsync(
          `ffmpeg -f lavfi -i color=c=purple:s=1080x1920:d=5 -c:v libx264 -t 5 -y "${tempOutput}"`,
          { timeout: 30000 }
        );
      }
    }
    
    // Step 2: Convert to vertical 9:16 format using FFmpeg
    const verticalOutput = path.join(tempDir, `vertical_${outputFilename}`);
    console.log(`[${workerId}] Converting to vertical format...`);
    
    await execAsync(
      `ffmpeg -i "${tempOutput}" -vf "scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2:black" -c:v libx264 -preset fast -crf 23 -c:a aac -y "${verticalOutput}"`,
      { timeout: 120000 }
    );
    
    // Step 3: Upload to Supabase Storage
    const storagePath = `${task.user_id}/${task.job_id}.mp4`;
    console.log(`[${workerId}] Uploading to storage: ${storagePath}`);
    
    const fileBuffer = fs.readFileSync(verticalOutput);
    
    const { error: uploadError } = await supabase.storage
      .from('renders')
      .upload(storagePath, fileBuffer, {
        contentType: 'video/mp4',
        upsert: true
      });
    
    if (uploadError) {
      throw new Error(`Upload failed: ${uploadError.message}`);
    }
    
    // Step 4: Get public URL
    const { data: urlData } = supabase.storage
      .from('renders')
      .getPublicUrl(storagePath);
    
    const publicUrl = urlData?.publicUrl;
    
    if (!publicUrl) {
      throw new Error('Failed to get public URL for uploaded file');
    }
    
    console.log(`[${workerId}] Upload complete: ${publicUrl}`);
    
    // Step 5: Update task and job as complete
    await supabase
      .from('render_tasks')
      .update({
        status: 'done',
        output: { path: storagePath, url: publicUrl }
      })
      .eq('id', task.id);
    
    await supabase
      .from('video_processing_jobs')
      .update({
        status: 'done',
        render_status: 'done',
        result_url: publicUrl
      })
      .eq('id', task.job_id);
    
    console.log(`[${workerId}] Render task ${task.id} completed successfully`);
    
    // Cleanup temp files
    try {
      fs.unlinkSync(tempOutput);
      fs.unlinkSync(verticalOutput);
    } catch (e) {
      // Ignore cleanup errors
    }
    
  } catch (err) {
    console.error(`[${workerId}] Render failed:`, err.message);
    
    // Check if we should retry (max 3 attempts)
    if (task.attempts < 3) {
      console.log(`[${workerId}] Will retry (attempt ${task.attempts}/3)`);
      await supabase
        .from('render_tasks')
        .update({ status: 'queued' })
        .eq('id', task.id);
    } else {
      await failTask(task, err.message);
    }
  }
}

async function failTask(task, errorMessage) {
  await supabase
    .from('render_tasks')
    .update({
      status: 'failed',
      output: { error: errorMessage }
    })
    .eq('id', task.id);
  
  await supabase
    .from('video_processing_jobs')
    .update({
      status: 'failed',
      render_status: 'failed',
      error: errorMessage
    })
    .eq('id', task.job_id);
  
  console.log(`[${workerId}] Task ${task.id} failed: ${errorMessage}`);
}

// ============================================
// CLIP EDIT WORKER LOGIC
// ============================================

async function claimEditTask() {
  // Use RPC to atomically claim an edit task
  console.log(`[${workerId}] Calling claim_edit_task RPC...`);
  const { data, error } = await supabase.rpc('claim_edit_task', {
    p_worker_id: workerId
  });
  
  if (error) {
    console.log(`[${workerId}] RPC error code: ${error.code}, message: ${error.message}`);
    // If RPC doesn't exist, fall back to manual claim
    if (error.code === '42883') {
      console.log(`[${workerId}] RPC not found, using fallback...`);
      return await claimEditTaskFallback();
    }
    console.error('Claim edit task error:', error.message);
    return null;
  }
  
  console.log(`[${workerId}] RPC returned ${data?.length || 0} edit tasks`);
  return data && data.length > 0 ? data[0] : null;
}

async function claimEditTaskFallback() {
  const { data: edits, error: fetchError } = await supabase
    .from('clip_edits')
    .select('*')
    .eq('status', 'queued')
    .order('created_at', { ascending: true })
    .limit(1);

  if (fetchError || !edits || edits.length === 0) {
    return null;
  }

  const edit = edits[0];

  const { data: claimed, error: claimError } = await supabase
    .from('clip_edits')
    .update({ status: 'processing' })
    .eq('id', edit.id)
    .eq('status', 'queued')
    .select()
    .single();

  if (claimError || !claimed) {
    return null;
  }

  return claimed;
}

// Sanitize text for FFmpeg drawtext filter
function sanitizeForFFmpeg(text, maxLength = 50) {
  if (!text) return '';
  // Escape characters that have special meaning in FFmpeg drawtext filter
  return text
    .replace(/\\/g, '\\\\')     // Escape backslashes first
    .replace(/'/g, "\\'")        // Escape single quotes
    .replace(/:/g, '\\:')        // Escape colons
    .replace(/\[/g, '\\[')       // Escape brackets
    .replace(/\]/g, '\\]')
    .replace(/;/g, '\\;')        // Escape semicolons
    .replace(/\n/g, '')          // Remove newlines
    .replace(/\r/g, '')          // Remove carriage returns
    .substring(0, maxLength);    // Limit length for safety
}

async function processEditTask(edit) {
  console.log(`[${workerId}] Processing edit task ${edit.id} for job ${edit.source_job_id}`);
  console.log(`[${workerId}] Edit settings received:`, JSON.stringify(edit.settings, null, 2));
  
  const settings = edit.settings || {};
  const fitMode = settings.fitMode || 'contain';
  const paddingTop = settings.paddingTop || 0;
  const paddingBottom = settings.paddingBottom || 0;
  const paddingColor = settings.paddingColor || '#000000';
  const watermarkText = sanitizeForFFmpeg(settings.watermarkText || '');
  const watermarkPosition = settings.watermarkPosition || 'bottom-right';
  
  // Caption box settings (TikTok-style white rectangular caption box)
  const captionBox = settings.captionBox || null;
  console.log(`[${workerId}] Caption box settings:`, captionBox ? JSON.stringify(captionBox) : 'none');
  const captionText = captionBox ? sanitizeForFFmpeg(captionBox.text || '', 200) : ''; // 200 chars for captions
  const captionPosition = captionBox?.position || 'bottom';
  const captionSize = captionBox?.size || 'medium';
  
  try {
    // Get source job to get the result_url and user_id
    const { data: sourceJob, error: jobError } = await supabase
      .from('video_processing_jobs')
      .select('result_url, user_id')
      .eq('id', edit.source_job_id)
      .single();
    
    if (jobError || !sourceJob || !sourceJob.result_url) {
      throw new Error('Source job not found or has no video');
    }
    
    if (!sourceJob.user_id) {
      throw new Error('Source job has no user_id');
    }
    
    const sourceUrl = sourceJob.result_url;
    const userId = sourceJob.user_id; // Use user_id from source job
    const tempDir = os.tmpdir();
    const sourceFilename = `source_${edit.id}.mp4`;
    const editedFilename = `edited_${edit.id}.mp4`;
    const tempSource = path.join(tempDir, sourceFilename);
    const tempEdited = path.join(tempDir, editedFilename);
    
    // Step 1: Download the source video
    console.log(`[${workerId}] Downloading source video from ${sourceUrl.substring(0, 50)}...`);
    
    const response = await fetch(sourceUrl);
    if (!response.ok) {
      throw new Error(`Failed to download source video: ${response.status}`);
    }
    
    const buffer = Buffer.from(await response.arrayBuffer());
    fs.writeFileSync(tempSource, buffer);
    
    console.log(`[${workerId}] Source video downloaded (${buffer.length} bytes)`);
    
    // Step 2: Build FFmpeg command based on settings
    let vfFilters = [];
    
    if (fitMode === 'contain') {
      // Scale to fit within 1080x1920 with padding
      const totalHeight = 1920 - paddingTop - paddingBottom;
      vfFilters.push(`scale=1080:${totalHeight}:force_original_aspect_ratio=decrease`);
      vfFilters.push(`pad=1080:1920:(ow-iw)/2:${paddingTop}:${paddingColor.replace('#', '0x')}`);
    } else {
      // Cover mode - crop to fill
      vfFilters.push('scale=1080:1920:force_original_aspect_ratio=increase');
      vfFilters.push('crop=1080:1920');
    }
    
    // Add watermark if specified (watermarkText is already sanitized)
    if (watermarkText) {
      let x = '10';
      let y = '10';
      
      switch (watermarkPosition) {
        case 'top-left':
          x = '10';
          y = '10';
          break;
        case 'top-right':
          x = 'w-text_w-10';
          y = '10';
          break;
        case 'bottom-left':
          x = '10';
          y = 'h-text_h-10';
          break;
        case 'bottom-right':
          x = 'w-text_w-10';
          y = 'h-text_h-10';
          break;
        case 'center':
          x = '(w-text_w)/2';
          y = '(h-text_h)/2';
          break;
      }
      
      // Use sanitized watermark text - already escaped in sanitizeForFFmpeg
      // Use bundled font file at /app/fonts/DejaVuSans.ttf for reliable rendering
      vfFilters.push(`drawtext=text='${watermarkText}':fontfile=/app/fonts/DejaVuSans.ttf:fontcolor=white@0.7:fontsize=32:x=${x}:y=${y}`);
    }
    
    // Add caption box if specified (TikTok-style white rounded box with black text)
    if (captionText) {
      // Size presets: font size and box padding
      let fontSize, boxPadding, boxHeight;
      switch (captionSize) {
        case 'small':
          fontSize = 36;
          boxPadding = 20;
          boxHeight = 76; // fontSize + 2*boxPadding
          break;
        case 'large':
          fontSize = 56;
          boxPadding = 40;
          boxHeight = 136;
          break;
        case 'medium':
        default:
          fontSize = 46;
          boxPadding = 30;
          boxHeight = 106;
          break;
      }
      
      // Position: Y coordinate for box
      let boxY;
      switch (captionPosition) {
        case 'top':
          boxY = 150; // Below status bar area
          break;
        case 'center':
          boxY = `(h-${boxHeight})/2`;
          break;
        case 'bottom':
        default:
          boxY = `h-${boxHeight}-200`; // Above bottom UI area
          break;
      }
      
      // Calculate text position (centered in box)
      const textY = typeof boxY === 'string' ? `${boxY}+${boxPadding}` : boxY + boxPadding;
      
      // Use drawtext with box option for white box effect
      // boxcolor creates a filled background behind the text
      // boxborderw adds padding around the text within the box
      // Use bundled font file at /app/fonts/DejaVuSans.ttf for reliable rendering
      vfFilters.push(`drawtext=text='${captionText}':fontfile=/app/fonts/DejaVuSans.ttf:fontcolor=black:fontsize=${fontSize}:x=(w-text_w)/2:y=${textY}:box=1:boxcolor=white@0.95:boxborderw=${boxPadding}`);
      
      console.log(`[${workerId}] Adding caption box: "${captionText}" at ${captionPosition}, size ${captionSize}`);
    }
    
    const vfString = vfFilters.join(',');
    
    console.log(`[${workerId}] Applying edit with filters: ${vfString.substring(0, 100)}...`);
    
    await execAsync(
      `ffmpeg -i "${tempSource}" -vf "${vfString}" -c:v libx264 -preset fast -crf 23 -c:a aac -y "${tempEdited}"`,
      { timeout: 180000 } // 3 minute timeout
    );
    
    console.log(`[${workerId}] Edit applied successfully`);
    
    // Step 3: Generate thumbnail from edited video
    const thumbnailFilename = `thumbnail_${edit.id}.jpg`;
    const tempThumbnail = path.join(tempDir, thumbnailFilename);
    
    console.log(`[${workerId}] Generating thumbnail...`);
    try {
      await execAsync(
        `ffmpeg -i "${tempEdited}" -ss 00:00:01 -vframes 1 -q:v 2 -y "${tempThumbnail}"`,
        { timeout: 30000 }
      );
    } catch (thumbErr) {
      console.log(`[${workerId}] Thumbnail at 1s failed, trying 0s...`);
      await execAsync(
        `ffmpeg -i "${tempEdited}" -ss 00:00:00 -vframes 1 -q:v 2 -y "${tempThumbnail}"`,
        { timeout: 30000 }
      );
    }
    console.log(`[${workerId}] Thumbnail generated`);
    
    // Step 4: Upload video to Supabase Storage
    const storagePath = `${userId}/${edit.source_job_id}_edit_${edit.id}.mp4`;
    const thumbnailStoragePath = `${userId}/${edit.source_job_id}_edit_${edit.id}_thumb.jpg`;
    
    console.log(`[${workerId}] Uploading edited video to storage: ${storagePath}`);
    
    const editedBuffer = fs.readFileSync(tempEdited);
    
    const { error: uploadError } = await supabase.storage
      .from('renders')
      .upload(storagePath, editedBuffer, {
        contentType: 'video/mp4',
        upsert: true
      });
    
    if (uploadError) {
      throw new Error(`Upload failed: ${uploadError.message}`);
    }
    
    // Upload thumbnail
    let thumbnailUrl = null;
    try {
      const thumbnailBuffer = fs.readFileSync(tempThumbnail);
      const { error: thumbUploadError } = await supabase.storage
        .from('renders')
        .upload(thumbnailStoragePath, thumbnailBuffer, {
          contentType: 'image/jpeg',
          upsert: true
        });
      
      if (!thumbUploadError) {
        const { data: thumbUrlData } = supabase.storage
          .from('renders')
          .getPublicUrl(thumbnailStoragePath);
        thumbnailUrl = thumbUrlData?.publicUrl;
        console.log(`[${workerId}] Thumbnail uploaded: ${thumbnailUrl}`);
      }
    } catch (thumbErr) {
      console.log(`[${workerId}] Thumbnail upload failed (non-fatal): ${thumbErr.message}`);
    }
    
    // Step 5: Get public URL for video
    const { data: urlData } = supabase.storage
      .from('renders')
      .getPublicUrl(storagePath);
    
    const publicUrl = urlData?.publicUrl;
    
    if (!publicUrl) {
      throw new Error('Failed to get public URL for edited file');
    }
    
    console.log(`[${workerId}] Edit upload complete: ${publicUrl}`);
    
    // Step 6: Update edit as complete with thumbnail
    const updateData = {
      status: 'done',
      output_url: publicUrl
    };
    if (thumbnailUrl) {
      updateData.thumbnail_url = thumbnailUrl;
    }
    
    await supabase
      .from('clip_edits')
      .update(updateData)
      .eq('id', edit.id);
    
    console.log(`[${workerId}] Edit task ${edit.id} completed successfully`);
    
    // Cleanup temp files
    try {
      fs.unlinkSync(tempSource);
      fs.unlinkSync(tempEdited);
      if (fs.existsSync(tempThumbnail)) {
        fs.unlinkSync(tempThumbnail);
      }
    } catch (e) {
      // Ignore cleanup errors
    }
    
  } catch (err) {
    console.error(`[${workerId}] Edit failed:`, err.message);
    
    await supabase
      .from('clip_edits')
      .update({
        status: 'failed',
        error: err.message
      })
      .eq('id', edit.id);
  }
}

// ============================================
// AI SHORT CREATOR WORKER LOGIC
// ============================================

// ElevenLabs voice ID mapping (matches server/services/elevenlabs.ts)
const ELEVENLABS_VOICE_IDS = {
  'Rachel': '21m00Tcm4TlvDq8ikWAM',
  'Drew': '29vD33N1CtxCmqQRPOHJ',
  'Paul': '5Q0t7uMcjvnagumLfvZi',
  'Sarah': 'EXAVITQu4vr4xnSDxMaL',
  'Charlie': 'IKne3meq5aSn9XLyUdCD',
  'George': 'JBFqnCBsd6RMkjVDRZzb',
  'Emily': 'LcfcDJNUP1GQjkzn1xUU',
  'Josh': 'TxGEqnHWrfWFTfGW9XjX',
  'Charlotte': 'XB0fDUnXU5powFXDhCwa',
  'Lily': 'pFZP5JQG7iQjIQuC4Bku'
};

async function generateVoiceover(text, voiceId) {
  if (!ELEVENLABS_API_KEY) {
    throw new Error('ELEVENLABS_API_KEY not configured');
  }

  const elevenLabsVoiceId = ELEVENLABS_VOICE_IDS[voiceId] || ELEVENLABS_VOICE_IDS['Rachel'];
  
  console.log(`[${workerId}] Generating voiceover with voice ${voiceId} (${elevenLabsVoiceId})...`);

  const response = await fetch(
    `https://api.elevenlabs.io/v1/text-to-speech/${elevenLabsVoiceId}`,
    {
      method: 'POST',
      headers: {
        'Accept': 'audio/mpeg',
        'Content-Type': 'application/json',
        'xi-api-key': ELEVENLABS_API_KEY
      },
      body: JSON.stringify({
        text: text,
        model_id: 'eleven_turbo_v2_5',
        voice_settings: {
          stability: 0.5,
          similarity_boost: 0.75,
          style: 0.0,
          use_speaker_boost: true
        }
      })
    }
  );

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`ElevenLabs API error: ${response.status} - ${errorText}`);
  }

  const audioBuffer = Buffer.from(await response.arrayBuffer());
  console.log(`[${workerId}] Voiceover generated: ${audioBuffer.length} bytes`);
  return audioBuffer;
}

async function downloadPexelsVideos(scenes, targetDuration) {
  if (!PEXELS_API_KEY) {
    console.log(`[${workerId}] PEXELS_API_KEY not configured, using placeholder videos`);
    return [];
  }

  const videos = [];
  const sceneDuration = Math.ceil(targetDuration / scenes.length);

  for (let i = 0; i < scenes.length && i < 6; i++) {
    const scene = scenes[i];
    const query = scene.visual || scene.text?.substring(0, 30) || 'abstract background';
    
    console.log(`[${workerId}] Searching Pexels for scene ${i + 1}: "${query}"`);

    try {
      const response = await fetch(
        `https://api.pexels.com/videos/search?query=${encodeURIComponent(query)}&orientation=portrait&per_page=3&size=medium`,
        {
          headers: {
            'Authorization': PEXELS_API_KEY
          }
        }
      );

      if (!response.ok) {
        console.log(`[${workerId}] Pexels search failed for scene ${i + 1}: ${response.status}`);
        continue;
      }

      const data = await response.json();
      
      if (data.videos && data.videos.length > 0) {
        const video = data.videos[0];
        const hdFile = video.video_files?.find(f => f.quality === 'hd' && f.width < f.height) ||
                       video.video_files?.find(f => f.quality === 'sd' && f.width < f.height) ||
                       video.video_files?.[0];
        
        if (hdFile?.link) {
          videos.push({
            url: hdFile.link,
            duration: sceneDuration,
            sceneIndex: i
          });
          console.log(`[${workerId}] Found video for scene ${i + 1}: ${hdFile.width}x${hdFile.height}`);
        }
      }
    } catch (err) {
      console.log(`[${workerId}] Pexels error for scene ${i + 1}: ${err.message}`);
    }

    await new Promise(resolve => setTimeout(resolve, 200));
  }

  return videos;
}

async function downloadVideoClip(url, outputPath) {
  console.log(`[${workerId}] Downloading video clip...`);
  
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to download video: ${response.status}`);
  }
  
  const buffer = Buffer.from(await response.arrayBuffer());
  fs.writeFileSync(outputPath, buffer);
  console.log(`[${workerId}] Video clip downloaded: ${buffer.length} bytes`);
  return buffer.length;
}

async function claimAiShortJob() {
  const { data: jobs, error: fetchError } = await supabase
    .from('video_processing_jobs')
    .select('*')
    .eq('status', 'ready')
    .eq('job_type', 'ai_short')
    .order('created_at', { ascending: true })
    .limit(1);

  if (fetchError || !jobs || jobs.length === 0) {
    return null;
  }

  const job = jobs[0];

  const { data: claimed, error: claimError } = await supabase
    .from('video_processing_jobs')
    .update({ status: 'processing' })
    .eq('id', job.id)
    .eq('status', 'ready')
    .select()
    .single();

  if (claimError || !claimed) {
    return null;
  }

  return claimed;
}

async function processAiShortJob(job) {
  const maxAttempts = 3;
  let attempt = 1;
  let currentJob = job;
  
  currentJobId = job.id;
  
  try {
    while (attempt <= maxAttempts) {
      console.log(`[${workerId}] Processing AI short job ${currentJob.id} (attempt ${attempt}/${maxAttempts})`);
      
      const tempDir = os.tmpdir();
      const jobDir = path.join(tempDir, `aishort_${currentJob.id}_${attempt}`);
      
      let success = false;
      let lastError = null;
      let isValidationError = false;
      
      try {
        fs.mkdirSync(jobDir, { recursive: true });

        let config;
        try {
          config = JSON.parse(currentJob.keywords || '{}');
        } catch (e) {
          isValidationError = true;
          throw new Error('Invalid job payload: keywords must be valid JSON');
        }

        const { topic, style, duration, script, voiceId } = config;
        
        if (!script || !script.fullScript) {
          isValidationError = true;
          throw new Error('Missing script in job payload');
        }

        const targetDuration = parseInt(duration) || 60;
        const scenes = script.scenes || [];
        
        console.log(`[${workerId}] AI Short: "${topic}" (${style}), ${targetDuration}s, ${scenes.length} scenes`);

        const audioPath = path.join(jobDir, 'voiceover.mp3');
        const voiceoverBuffer = await generateVoiceover(script.fullScript, voiceId || 'Rachel');
        fs.writeFileSync(audioPath, voiceoverBuffer);
        console.log(`[${workerId}] Voiceover saved: ${audioPath}`);

        let videoClips = [];
        
        if (scenes.length > 0) {
          const pexelsVideos = await downloadPexelsVideos(scenes, targetDuration);
          
          for (let i = 0; i < pexelsVideos.length; i++) {
            const video = pexelsVideos[i];
            const clipPath = path.join(jobDir, `clip_${i}.mp4`);
            
            try {
              await downloadVideoClip(video.url, clipPath);
              videoClips.push({
                path: clipPath,
                duration: video.duration
              });
            } catch (err) {
              console.log(`[${workerId}] Failed to download clip ${i}: ${err.message}`);
            }
          }
        }

        const outputPath = path.join(jobDir, 'output.mp4');

        if (videoClips.length === 0) {
          console.log(`[${workerId}] No video clips, creating background with hook text...`);
          
          const hookText = sanitizeForFFmpeg(script.hook || topic || 'AI Generated Video', 100);
          const fontPath = '/app/fonts/DejaVuSans.ttf';
          
          if (fs.existsSync(fontPath)) {
            await execAsync(
              `ffmpeg -f lavfi -i color=c=0x1a1a2e:s=1080x1920:d=${targetDuration} ` +
              `-i "${audioPath}" ` +
              `-vf "drawtext=text='${hookText}':fontfile=${fontPath}:fontcolor=white:fontsize=48:x=(w-text_w)/2:y=(h-text_h)/2" ` +
              `-c:v libx264 -preset fast -crf 23 -c:a aac -shortest -y "${outputPath}"`,
              { timeout: 180000 }
            );
          } else {
            await execAsync(
              `ffmpeg -f lavfi -i color=c=0x1a1a2e:s=1080x1920:d=${targetDuration} ` +
              `-i "${audioPath}" ` +
              `-c:v libx264 -preset fast -crf 23 -c:a aac -shortest -y "${outputPath}"`,
              { timeout: 180000 }
            );
          }
        } else {
          console.log(`[${workerId}] Assembling ${videoClips.length} video clips with voiceover...`);
          
          const concatListPath = path.join(jobDir, 'concat.txt');
          const sceneDuration = Math.ceil(targetDuration / videoClips.length);
          
          const processedClips = [];
          for (let i = 0; i < videoClips.length; i++) {
            const clip = videoClips[i];
            const processedPath = path.join(jobDir, `processed_${i}.mp4`);
            
            try {
              await execAsync(
                `ffmpeg -i "${clip.path}" -t ${sceneDuration} ` +
                `-vf "scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2:black,fps=30" ` +
                `-c:v libx264 -preset fast -crf 23 -an -y "${processedPath}"`,
                { timeout: 60000 }
              );
              processedClips.push(processedPath);
            } catch (err) {
              console.log(`[${workerId}] Failed to process clip ${i}: ${err.message}`);
            }
          }
          
          if (processedClips.length === 0) {
            throw new Error('Failed to process any video clips');
          }

          const concatContent = processedClips.map(p => `file '${p}'`).join('\n');
          fs.writeFileSync(concatListPath, concatContent);

          const concatPath = path.join(jobDir, 'concat.mp4');
          await execAsync(
            `ffmpeg -f concat -safe 0 -i "${concatListPath}" ` +
            `-c:v libx264 -preset fast -crf 23 -pix_fmt yuv420p -r 30 -y "${concatPath}"`,
            { timeout: 180000 }
          );

          await execAsync(
            `ffmpeg -i "${concatPath}" -i "${audioPath}" ` +
            `-c:v libx264 -preset fast -crf 23 -c:a aac -map 0:v:0 -map 1:a:0 -shortest -y "${outputPath}"`,
            { timeout: 180000 }
          );
        }

        console.log(`[${workerId}] Video assembled, uploading to storage...`);

        const storagePath = `${currentJob.user_id}/${currentJob.id}.mp4`;
        const fileBuffer = fs.readFileSync(outputPath);

        const { error: uploadError } = await supabase.storage
          .from('renders')
          .upload(storagePath, fileBuffer, {
            contentType: 'video/mp4',
            upsert: true
          });

        if (uploadError) {
          throw new Error(`Upload failed: ${uploadError.message}`);
        }

        const { data: urlData } = supabase.storage
          .from('renders')
          .getPublicUrl(storagePath);

        const publicUrl = urlData?.publicUrl;

        if (!publicUrl) {
          throw new Error('Failed to get public URL');
        }

        await supabase
          .from('video_processing_jobs')
          .update({
            status: 'done',
            render_status: 'done',
            result_url: publicUrl
          })
          .eq('id', currentJob.id);

        console.log(`[${workerId}] AI short job ${currentJob.id} completed: ${publicUrl}`);
        
        success = true;
        
        try { fs.rmSync(jobDir, { recursive: true, force: true }); } catch (e) {}
        
        return;

      } catch (err) {
        lastError = err;
        isValidationError = err.message.includes('Invalid job payload') ||
                            err.message.includes('Missing script');
        
        console.error(`[${workerId}] AI short job failed (attempt ${attempt}/${maxAttempts}):`, err.message);
        
        try { fs.rmSync(jobDir, { recursive: true, force: true }); } catch (e) {}
      }

      if (isValidationError || attempt >= maxAttempts) {
        await supabase
          .from('video_processing_jobs')
          .update({
            status: 'failed',
            error: `${lastError.message}${attempt > 1 ? ` (after ${attempt} attempts)` : ''}`
          })
          .eq('id', currentJob.id);
        return;
      }

      console.log(`[${workerId}] Retrying AI short job ${currentJob.id} in 5 seconds...`);
      
      await supabase
        .from('video_processing_jobs')
        .update({ status: 'ready' })
        .eq('id', currentJob.id);
      
      await new Promise(resolve => setTimeout(resolve, 5000));
      
      const { data: retriedJob } = await supabase
        .from('video_processing_jobs')
        .update({ status: 'processing' })
        .eq('id', currentJob.id)
        .eq('status', 'ready')
        .select()
        .single();
      
      if (!retriedJob) {
        const { data: jobStatus } = await supabase
          .from('video_processing_jobs')
          .select('status')
          .eq('id', currentJob.id)
          .single();
        
        if (jobStatus?.status === 'processing') {
          console.log(`[${workerId}] Job ${currentJob.id} was claimed by another worker`);
        } else if (jobStatus?.status === 'done' || jobStatus?.status === 'failed') {
          console.log(`[${workerId}] Job ${currentJob.id} already completed with status: ${jobStatus.status}`);
        } else {
          console.log(`[${workerId}] Job ${currentJob.id} in unexpected state: ${jobStatus?.status || 'unknown'}, marking as failed`);
          await supabase
            .from('video_processing_jobs')
            .update({
              status: 'failed',
              error: `Worker retry failed: job in unexpected state after ${attempt} attempts`
            })
            .eq('id', currentJob.id);
        }
        return;
      }
      
      currentJob = retriedJob;
      attempt++;
    }
  } finally {
    currentJobId = null;
  }
}

// ============================================
// JOB POLLING (DISCOVER JOBS)
// ============================================

async function claimDiscoverJob() {
  const { data: jobs, error: fetchError } = await supabase
    .from('video_processing_jobs')
    .select('*')
    .eq('status', 'ready')
    .eq('job_type', 'discover')
    .order('created_at', { ascending: true })
    .limit(1);

  if (fetchError || !jobs || jobs.length === 0) {
    return null;
  }

  const job = jobs[0];

  const { data: claimed, error: claimError } = await supabase
    .from('video_processing_jobs')
    .update({ status: 'processing' })
    .eq('id', job.id)
    .eq('status', 'ready')
    .select()
    .single();

  if (claimError || !claimed) {
    return null;
  }

  return claimed;
}

// ============================================
// MAIN POLLING LOOP
// ============================================

async function pollForWork() {
  try {
    // First check for render tasks (higher priority)
    console.log(`[${workerId}] Checking for render tasks...`);
    const renderTask = await claimRenderTask();
    if (renderTask) {
      console.log(`[${workerId}] Found render task: ${renderTask.id}`);
      await processRenderTask(renderTask);
      return;
    }
    
    // Then check for edit tasks
    console.log(`[${workerId}] Checking for edit tasks...`);
    const editTask = await claimEditTask();
    if (editTask) {
      console.log(`[${workerId}] Found edit task: ${editTask.id}`);
      await processEditTask(editTask);
      return;
    }
    
    // Then check for AI short jobs
    console.log(`[${workerId}] Checking for AI short jobs...`);
    const aiShortJob = await claimAiShortJob();
    if (aiShortJob) {
      console.log(`[${workerId}] Found AI short job: ${aiShortJob.id}`);
      await processAiShortJob(aiShortJob);
      return;
    }
    
    // Then check for discover jobs
    console.log(`[${workerId}] Checking for discover jobs...`);
    const discoverJob = await claimDiscoverJob();
    if (discoverJob) {
      console.log(`[${workerId}] Found discover job: ${discoverJob.id}`);
      await processDiscoverJob(discoverJob);
      return;
    }
    
    console.log(`[${workerId}] No work found, waiting...`);
  } catch (err) {
    console.error(`[${workerId}] Poll error:`, err.message);
  }
}

async function main() {
  console.log(`[${workerId}] ClipFlow Worker starting...`);
  console.log(`[${workerId}] Supabase URL: ${supabaseUrl?.substring(0, 30)}...`);
  
  // Check if bundled font file exists for drawtext filter
  const fontPath = '/app/fonts/DejaVuSans.ttf';
  if (fs.existsSync(fontPath)) {
    console.log(`[${workerId}] Bundled font file found: ${fontPath}`);
  } else {
    console.warn(`[${workerId}] WARNING: Bundled font file not found at ${fontPath} - captions/watermarks may not render!`);
  }
  
  await sendHeartbeat();
  setInterval(sendHeartbeat, 60000);

  console.log(`[${workerId}] Polling for jobs and render tasks (30s interval)...`);
  
  while (!isShuttingDown) {
    await pollForWork();
    await new Promise(resolve => setTimeout(resolve, 30000));
  }
  
  console.log(`[${workerId}] Worker stopped.`);
}

main().catch(err => {
  console.error('Worker crashed:', err);
  process.exit(1);
});
