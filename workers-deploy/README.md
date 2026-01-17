# ClipFlow Workers

Background workers for ClipFlow video processing.

## Quick Deploy to Railway

### Step 1: Create a New GitHub Repository

1. Go to https://github.com/new
2. Name it `clipflow-workers`
3. Make it Public or Private
4. Click "Create repository"

### Step 2: Upload These Files

Upload all files from this folder to your new repo:
- `package.json`
- `Dockerfile`
- `src/discover.js`
- `src/render.js`
- `.env.example` (optional)

You can drag and drop files directly on GitHub, or use git:
```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/YOUR_USERNAME/clipflow-workers.git
git push -u origin main
```

### Step 3: Deploy to Railway

1. Go to https://railway.app and sign in
2. Click **"New Project"**
3. Select **"Deploy from GitHub repo"**
4. Choose your `clipflow-workers` repository
5. Railway will auto-detect the Dockerfile and build

### Step 4: Add Environment Variables

In Railway, go to your service → Variables tab and add:

| Variable | Value |
|----------|-------|
| `SUPABASE_URL` | Your Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Your Supabase service role key |
| `WORKER_ID` | `discover-1` (or `render-1` for render worker) |

### Step 5: Deploy Render Worker (Optional)

To run both workers:

1. In Railway, click **"New"** → **"Service"**
2. Select the same GitHub repo
3. Go to Settings → Deploy
4. Change **Start Command** to: `npm run render`
5. Add the same environment variables (use `WORKER_ID=render-1`)

## Files Structure

```
clipflow-workers/
├── package.json      # No build step, just Node.js
├── Dockerfile        # Uses Node 20 Alpine
├── src/
│   ├── discover.js   # Discover worker
│   └── render.js     # Render worker
└── .env.example      # Environment template
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SUPABASE_URL` | Yes | Your Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Yes | Service role key (not anon key!) |
| `WORKER_ID` | No | Unique worker identifier (default: discover-1 or render-1) |

## How It Works

- **Discover Worker**: Polls for `discover` jobs, simulates finding viral clips
- **Render Worker**: Claims render tasks via RPC, simulates video processing
- Both workers send heartbeats every 60 seconds to `worker_heartbeat` table

## Troubleshooting

**Build fails?**
- Make sure you uploaded all files including the `src/` folder
- Check that `Dockerfile` is in the root of the repo

**Worker not processing jobs?**
- Check Railway logs for errors
- Verify environment variables are set correctly
- Ensure Supabase tables exist (run migrations first)
