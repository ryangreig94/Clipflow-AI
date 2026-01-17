import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const workerId = process.env.WORKER_ID || 'render-1';

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
        worker_type: 'render',
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

async function claimRenderTask() {
  const { data, error } = await supabase.rpc('claim_render_task', { p_worker_id: workerId });
  
  if (error) {
    if (!error.message.includes('No tasks available')) {
      console.error('Claim error:', error.message);
    }
    return null;
  }
  
  // RPC returns array, get first element
  if (Array.isArray(data) && data.length > 0) {
    return data[0];
  }
  
  return null;
}

async function processRenderTask(task) {
  console.log(`Processing render task ${task.id}`);
  
  try {
    await new Promise(resolve => setTimeout(resolve, 5000));

    const mockOutputUrl = `https://example-storage.supabase.co/renders/${task.id}.mp4`;

    await supabase
      .from('render_tasks')
      .update({
        status: 'done',
        updated_at: new Date().toISOString(),
        output: { url: mockOutputUrl }
      })
      .eq('id', task.id);

    console.log(`Completed render task ${task.id}`);
  } catch (err) {
    console.error(`Error processing task ${task.id}:`, err.message);
    
    await supabase
      .from('render_tasks')
      .update({ 
        status: 'failed',
        output: { error: err.message },
        worker_id: null 
      })
      .eq('id', task.id);
  }
}

async function pollForTasks() {
  console.log(`[${workerId}] Polling for render tasks...`);
  
  const task = await claimRenderTask();
  
  if (task) {
    await processRenderTask(task);
  }
}

async function main() {
  console.log(`Render Worker ${workerId} starting...`);
  
  await sendHeartbeat();
  setInterval(sendHeartbeat, 60000);

  while (true) {
    await pollForTasks();
    await new Promise(resolve => setTimeout(resolve, 5000));
  }
}

main().catch(err => {
  console.error('Worker crashed:', err);
  process.exit(1);
});
