import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const workerId = process.env.WORKER_ID || 'discover-1';

if (!supabaseUrl || !supabaseKey) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

async function sendHeartbeat() {
  try {
    const { error } = await supabase
      .from('worker_heartbeat')
      .upsert({
        worker_id: workerId,
        worker_type: 'discover',
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

async function claimJob() {
  // Atomically claim a job by updating it
  const { data: jobs, error: fetchError } = await supabase
    .from('video_processing_jobs')
    .select('*')
    .eq('status', 'ready')
    .in('job_type', ['discover', 'long_form'])
    .order('created_at', { ascending: true })
    .limit(1);

  if (fetchError) {
    console.error('Fetch error:', fetchError.message);
    return null;
  }

  if (!jobs || jobs.length === 0) {
    return null;
  }

  const job = jobs[0];

  // Try to claim it by updating status
  const { data: claimed, error: claimError } = await supabase
    .from('video_processing_jobs')
    .update({ status: 'processing' })
    .eq('id', job.id)
    .eq('status', 'ready') // Only if still ready (optimistic lock)
    .select()
    .single();

  if (claimError || !claimed) {
    // Another worker claimed it
    return null;
  }

  return claimed;
}

async function processJob(job) {
  console.log(`[${workerId}] Processing ${job.job_type} job ${job.id}`);
  
  try {
    // Simulate processing time
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Generate mock results based on job type
    let resultData;
    
    if (job.job_type === 'discover') {
      resultData = {
        clips: [
          { title: 'Viral Moment #1', platform: job.source_platform || 'twitch', duration: 45, viralScore: 94 },
          { title: 'Viral Moment #2', platform: job.source_platform || 'twitch', duration: 32, viralScore: 87 },
          { title: 'Viral Moment #3', platform: job.source_platform || 'twitch', duration: 58, viralScore: 91 }
        ],
        discoveredCount: 3
      };
    } else {
      resultData = {
        clips: [
          { title: 'Clip 1', startTime: 0, endTime: 45 },
          { title: 'Clip 2', startTime: 120, endTime: 180 }
        ]
      };
    }

    // Update job as completed
    const { error: updateError } = await supabase
      .from('video_processing_jobs')
      .update({
        status: 'done',
        render_status: 'done',
        result_url: JSON.stringify(resultData)
      })
      .eq('id', job.id);

    if (updateError) {
      throw new Error(updateError.message);
    }

    console.log(`[${workerId}] Completed job ${job.id}`);
  } catch (err) {
    console.error(`[${workerId}] Error processing job ${job.id}:`, err.message);
    
    await supabase
      .from('video_processing_jobs')
      .update({ 
        status: 'failed', 
        error: err.message
      })
      .eq('id', job.id);
  }
}

async function pollForJobs() {
  const job = await claimJob();
  
  if (job) {
    await processJob(job);
  }
}

async function main() {
  console.log(`[${workerId}] Discover Worker starting...`);
  
  await sendHeartbeat();
  setInterval(sendHeartbeat, 60000);

  while (true) {
    await pollForJobs();
    await new Promise(resolve => setTimeout(resolve, 5000));
  }
}

main().catch(err => {
  console.error('Worker crashed:', err);
  process.exit(1);
});
