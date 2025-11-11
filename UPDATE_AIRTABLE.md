# Post-Deployment: Updating Airtable

After deploying to Google Cloud Run, you need to update your Airtable base to use the new Cloud Run URL instead of ngrok.

## Your New Cloud Run URL

After deployment, you'll get a URL like:
```
https://company-research-agent-xxxxx-uc.a.run.app
```

Save this URL - you'll need it in multiple places!

## Step 1: Update Airtable Extension

The Email Generator extension needs to point to your Cloud Run service.

### Instructions:

1. **Open your Airtable base**
   - Go to https://airtable.com
   - Open the base with your Company Research Agent

2. **Open the Email Generator extension**
   - Look for the extension in the right sidebar
   - If not visible, click **Extensions** → **Email Generator**

3. **Access Settings**
   - Click the **⚙️ Settings** button (usually in top right corner)

4. **Update API Endpoint**
   - Find the "API Endpoint" field
   - **OLD VALUE**: `https://xxx.ngrok-free.app` (or similar)
   - **NEW VALUE**: `https://company-research-agent-xxxxx-uc.a.run.app`
   - ⚠️ **No trailing slash!**

5. **Save**
   - Click **Save** or the save button
   - Settings should persist

6. **Test**
   - Click "Refresh Templates" button
   - You should see your templates load
   - If it works, you're done with the extension! ✅

### Troubleshooting Extension

**"Failed to fetch templates"**
- Double-check URL has no trailing slash
- Verify URL is correct (copy from Cloud Run console)
- Check service is deployed and healthy

**"Network error"**
- Service might still be starting (wait 30 seconds)
- Check Cloud Run logs: `gcloud run logs tail company-research-agent --region us-central1`

---

## Step 2: Update Airtable Automations

Any automations that trigger research need to use the new URL.

### Find Your Automations:

1. **Open Automations**
   - In Airtable, click **Automations** in top menu

2. **Identify Research Automations**
   - Look for automations with "Webhook" or "HTTP" actions
   - Common names:
     - "Trigger Research on Status Change"
     - "Start Research on New Company"
     - "Queue Research Job"

### Update Each Automation:

For **EACH** automation that calls your webhook:

1. **Edit the automation**
   - Click the automation name
   - Click **Edit** button

2. **Find the Webhook Action**
   - Look for action type: "Send a request to URL" or "Webhook"
   - Click to expand it

3. **Update the URL**
   - **OLD**: `https://xxx.ngrok-free.app/webhook/start-research`
   - **NEW**: `https://company-research-agent-xxxxx-uc.a.run.app/webhook/start-research`

4. **Verify other settings remain:**
   ```
   Method: POST
   Content-Type: application/json
   Body: 
   {
     "company": {{Company Name}},
     "airtable_record_id": {{Record ID}},
     "google_drive_folder_url": {{Research Folder URL}},
     "use_local_context": true
   }
   ```

5. **Save the automation**

6. **Test it**
   - Find a test record
   - Manually trigger the automation
   - Check if research starts (look at record status)
   - Check Cloud Run logs for activity

7. **Repeat** for all other automations

### Common Automation Endpoints:

Update these URLs in your automations:

| Old (ngrok) | New (Cloud Run) |
|-------------|-----------------|
| `https://xxx.ngrok-free.app/webhook/start-research` | `https://YOUR-SERVICE.run.app/webhook/start-research` |
| `https://xxx.ngrok-free.app/generate-outreach` | `https://YOUR-SERVICE.run.app/generate-outreach` |
| `https://xxx.ngrok-free.app/generate-pdf` | `https://YOUR-SERVICE.run.app/generate-pdf` |

---

## Step 3: Verify Everything Works

### Test Checklist:

1. **Extension Test**
   - [ ] Open Email Generator extension
   - [ ] Click "Refresh Templates" → templates load ✅
   - [ ] Select a record with research data
   - [ ] Click "Generate Email"
   - [ ] Email appears in Draft field ✅

2. **Automation Test**
   - [ ] Find a test company record
   - [ ] Change status to trigger automation
   - [ ] Watch "Research Status" field update:
     - Queued → In Progress → Collecting Data → ... → Completed
   - [ ] Check Google Drive folder for PDF and JSON files ✅
   - [ ] Verify Airtable record has all data filled in ✅

3. **Manual Webhook Test** (optional, for debugging)
   ```bash
   curl -X POST https://YOUR-SERVICE.run.app/webhook/start-research \
     -H "Content-Type: application/json" \
     -d '{
       "company": "Test Company",
       "airtable_record_id": "recXXXXXXXXXX",
       "google_drive_folder_url": "https://drive.google.com/drive/folders/YOUR_FOLDER_ID"
     }'
   ```

4. **Check Logs** (if issues)
   ```bash
   gcloud run logs tail company-research-agent --region us-central1
   ```

---

## Step 4: Update Team Documentation

### Share with your team:

1. **Service URL**
   ```
   Production API: https://company-research-agent-xxxxx-uc.a.run.app
   ```

2. **Available Endpoints**
   - Research: `/webhook/start-research`
   - Email Gen: `/generate-outreach`
   - PDF Gen: `/generate-pdf`
   - Templates: `/templates`
   - Health: `/health`

3. **Monitoring**
   - Logs: `gcloud run logs tail company-research-agent --region us-central1`
   - Console: https://console.cloud.google.com/run

4. **Deployment Date**
   - Deployed: [DATE]
   - Deployed by: [NAME]

---

## Step 5: Decommission ngrok (Optional)

Once everything works with Cloud Run:

1. **Stop ngrok**
   ```bash
   # Find ngrok process
   ps aux | grep ngrok
   
   # Kill it
   kill <PID>
   ```

2. **Stop local server** (if running)
   ```bash
   # Ctrl+C to stop uvicorn
   ```

3. **Archive ngrok URL** (for records)
   - Document old URL for reference
   - Note when it was decommissioned

---

## Quick Reference Card

### Before (Development):
```
Extension:   https://xxx.ngrok-free.app
Automations: https://xxx.ngrok-free.app/webhook/start-research
```

### After (Production):
```
Extension:   https://company-research-agent-xxxxx-uc.a.run.app
Automations: https://company-research-agent-xxxxx-uc.a.run.app/webhook/start-research
```

### Testing:
```bash
# Get your service URL
gcloud run services describe company-research-agent \
  --region us-central1 \
  --format 'value(status.url)'

# Test health
curl https://YOUR-SERVICE.run.app/health

# View logs
gcloud run logs tail company-research-agent --region us-central1
```

---

## Troubleshooting

### Extension won't connect
1. Check URL has no trailing slash
2. Verify service is deployed: `gcloud run services list`
3. Check service is healthy: `curl https://YOUR-SERVICE.run.app/health`
4. Look at Cloud Run logs for errors

### Automation not triggering
1. Verify automation is turned **ON**
2. Check URL is exactly right (no typos)
3. Test automation manually with test record
4. Check "Runs" tab in automation for errors
5. View Cloud Run logs during test run

### Research fails partway through
1. Check Cloud Run logs: `gcloud run logs tail company-research-agent --region us-central1`
2. Look for Python errors or API failures
3. Common causes:
   - Invalid API keys
   - Tavily rate limit
   - Timeout (increase with `--timeout 1200`)
4. Verify secrets are correct: `gcloud secrets versions access latest --secret=research-env`

### Need to rollback?
```bash
# List revisions
gcloud run revisions list --service company-research-agent --region us-central1

# Rollback to previous
gcloud run services update-traffic company-research-agent \
  --region us-central1 \
  --to-revisions PREVIOUS_REVISION=100
```

---

## Success! 🎉

Once you've completed all steps:

- ✅ Extension uses Cloud Run URL
- ✅ All automations updated
- ✅ End-to-end test passed
- ✅ Team notified
- ✅ Old ngrok decommissioned

**Your system is now running on production infrastructure!**

Questions? Check:
- [GCLOUD_DEPLOYMENT.md](GCLOUD_DEPLOYMENT.md) - Full deployment guide
- [GCLOUD_COMMANDS.md](GCLOUD_COMMANDS.md) - Useful commands
- [DEPLOYMENT_CHECKLIST.md](DEPLOYMENT_CHECKLIST.md) - Complete checklist

Need help? View logs:
```bash
gcloud run logs tail company-research-agent --region us-central1 --format json
```
