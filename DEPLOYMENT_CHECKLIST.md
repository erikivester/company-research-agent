# Pre-Deployment Checklist

Use this checklist before deploying to Google Cloud Run.

## ☐ Prerequisites

- [ ] Google Cloud account created
- [ ] Billing enabled on GCP project
- [ ] Google Cloud SDK installed (`gcloud --version`)
- [ ] Authenticated with gcloud (`gcloud auth login`)
- [ ] Project selected (`gcloud config set project YOUR_PROJECT_ID`)

## ☐ Configuration Files

### .env File
- [ ] Copied from `.env.example`
- [ ] All API keys filled in:
  - [ ] `TAVILY_API_KEY`
  - [ ] `OPENAI_API_KEY`
  - [ ] `ANTHROPIC_API_KEY`
  - [ ] `GEMINI_API_KEY`
  - [ ] `AIRTABLE_API_KEY`
  - [ ] `AIRTABLE_BASE_ID`
  - [ ] `AIRTABLE_TABLE_NAME`
- [ ] Security keys generated:
  - [ ] `JWT_SECRET_KEY` (strong random string)
  - [ ] `API_KEY` (strong random string)
- [ ] Feature flags configured:
  - [ ] `USE_MOCK_DATA=false`
  - [ ] `ENABLE_AIRTABLE_UPLOAD=true`
  - [ ] `ENABLE_GDRIVE_UPLOAD=true`

### Google Drive Credentials
- [ ] `gdrive_credentials.json` file exists in project root
- [ ] Service account created in GCP Console
- [ ] JSON key downloaded
- [ ] Google Drive folders shared with service account email

### Optional: MongoDB
- [ ] MongoDB cluster created (if using)
- [ ] `MONGO_URI` configured
- [ ] `MONGODB_DB_NAME` set

## ☐ Local Testing

- [ ] Application runs locally: `python application.py`
- [ ] Health endpoint works: `curl http://localhost:8000/health`
- [ ] Research webhook tested locally
- [ ] Email generation tested locally
- [ ] PDF generation tested locally
- [ ] No errors in local logs

### Docker Testing (Recommended)
- [ ] Docker installed
- [ ] Build successful: `docker build -t company-research-agent .`
  - Note: This builds from `Dockerfile` (not `Dockerfile.airtable`)
  - `Dockerfile.airtable` is only for local Airtable extension development
- [ ] Container runs: `docker run -p 8000:8000 --env-file .env company-research-agent`
- [ ] Health check passes in container

## ☐ Google Cloud Setup

### Enable APIs
- [ ] Cloud Run API: `gcloud services enable run.googleapis.com`
- [ ] Secret Manager API: `gcloud services enable secretmanager.googleapis.com`
- [ ] Cloud Build API: `gcloud services enable cloudbuild.googleapis.com`
- [ ] Container Registry API: `gcloud services enable containerregistry.googleapis.com`

### Create Secrets
- [ ] Environment secret created: `gcloud secrets create research-env --data-file=.env`
- [ ] GDrive secret created: `gcloud secrets create gdrive-credentials --data-file=gdrive_credentials.json`
- [ ] Secrets visible: `gcloud secrets list`

### Grant Permissions
- [ ] Project number obtained
- [ ] Secret access granted to Cloud Run service account
- [ ] Verified with: `gcloud secrets get-iam-policy research-env`

## ☐ Deployment

### Deploy Service
- [ ] Deployment script is executable: `chmod +x deploy-gcloud.sh`
- [ ] Run setup: `./deploy-gcloud.sh --setup --project-id YOUR_PROJECT_ID`
- [ ] Deploy: `./deploy-gcloud.sh`
- [ ] Deployment successful (no errors)
- [ ] Service URL obtained

### Verify Deployment
- [ ] Service shows "HEALTHY" status in Cloud Console
- [ ] Health endpoint responds: `curl https://YOUR-SERVICE-URL/health`
- [ ] Templates endpoint works: `curl https://YOUR-SERVICE-URL/templates`
- [ ] No errors in Cloud Run logs

## ☐ Post-Deployment Configuration

### Update Airtable Extension
- [ ] Opened Airtable base
- [ ] Opened Email Generator extension
- [ ] Clicked Settings (⚙️)
- [ ] Updated API Endpoint to Cloud Run URL
- [ ] Tested "Refresh Templates" button
- [ ] Tested "Generate Email" with one record
- [ ] Email generated successfully

### Update Airtable Automations
For each automation:
- [ ] Automation 1: Webhook URL updated
- [ ] Automation 2: Webhook URL updated
- [ ] Automation 3: Webhook URL updated
- [ ] Test runs successful

### Update Webhook Callers
Update any other systems calling your webhooks:
- [ ] System 1: URL updated
- [ ] System 2: URL updated
- [ ] System 3: URL updated

## ☐ End-to-End Testing

### Research Webhook Test
- [ ] Trigger research from Airtable automation
- [ ] Check Cloud Run logs for activity
- [ ] Verify research completes successfully
- [ ] Check Airtable record updated with results
- [ ] PDF uploaded to Google Drive
- [ ] JSON context uploaded to Google Drive

### Email Generation Test
- [ ] Generate email from Airtable extension
- [ ] Email draft saved to Airtable
- [ ] Quality of generated email is good
- [ ] Uses correct template

### Batch Processing Test
- [ ] Select 5 records in extension
- [ ] Generate emails for all
- [ ] All 5 emails generated successfully
- [ ] Progress bar worked correctly

## ☐ Monitoring Setup

### Cloud Monitoring
- [ ] Open Cloud Run service in Console
- [ ] Review METRICS tab
- [ ] Set up alert for errors (optional)
- [ ] Set up alert for latency (optional)

### Logging
- [ ] Can view logs: `gcloud run logs tail company-research-agent --region us-central1`
- [ ] Logs show expected activity
- [ ] No unexpected errors

### Billing
- [ ] Budget alert created (recommended: $50-100/month)
- [ ] Billing email notifications enabled
- [ ] Cost estimate reviewed

## ☐ Documentation

- [ ] Service URL documented
- [ ] Team members notified of new URL
- [ ] Deployment date recorded
- [ ] Any custom configurations documented

## ☐ Security Review

- [ ] Service uses `--allow-unauthenticated` (or authentication configured)
- [ ] Secrets not committed to git
- [ ] `.env` in `.gitignore`
- [ ] `gdrive_credentials.json` in `.gitignore`
- [ ] CORS origins restricted to Airtable domains
- [ ] API keys have appropriate permissions only

## ☐ Rollback Plan

In case of issues:
- [ ] Know how to view logs: `gcloud run logs tail ...`
- [ ] Know how to redeploy: `./deploy-gcloud.sh`
- [ ] Have backup of working `.env` file
- [ ] Can rollback to previous revision if needed
- [ ] Have ngrok URL as fallback (for emergencies)

## ☐ Success Criteria

All must pass:
- [ ] ✅ Service deploys without errors
- [ ] ✅ Health check returns 200 OK
- [ ] ✅ Can fetch templates list
- [ ] ✅ Can generate test email
- [ ] ✅ Full research job completes successfully
- [ ] ✅ Airtable extension works with new URL
- [ ] ✅ No errors in Cloud Run logs
- [ ] ✅ Costs are within expected range

---

## Deployment Sign-Off

**Deployed by:** _________________  
**Date:** _________________  
**Service URL:** _________________  
**Project ID:** _________________  
**Region:** _________________  

**Notes:**
_________________________________________
_________________________________________
_________________________________________

---

## Quick Access Links

After deployment, save these URLs:

- **Cloud Run Console:** https://console.cloud.google.com/run
- **Service URL:** `gcloud run services describe company-research-agent --region us-central1 --format 'value(status.url)'`
- **Logs:** https://console.cloud.google.com/logs
- **Secrets:** https://console.cloud.google.com/security/secret-manager
- **Billing:** https://console.cloud.google.com/billing

---

**Ready to deploy?** Run: `./deploy-gcloud.sh --setup --project-id YOUR_PROJECT_ID`
