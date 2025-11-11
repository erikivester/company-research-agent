# Google Cloud Run Deployment Guide

This guide walks you through deploying the Company Research Agent to Google Cloud Run as a **single service** with multiple webhooks.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                 GOOGLE CLOUD RUN                    │
│                                                     │
│  ┌───────────────────────────────────────────────┐ │
│  │  company-research-agent (Single Service)      │ │
│  │                                                │ │
│  │  FastAPI Application                          │ │
│  │  ├── /webhook/start-research                  │ │
│  │  ├── /webhook/debug/*                         │ │
│  │  ├── /generate-outreach                       │ │
│  │  ├── /generate-pdf                            │ │
│  │  ├── /templates                               │ │
│  │  ├── /auth/token                              │ │
│  │  └── /health                                  │ │
│  │                                                │ │
│  │  Resources:                                    │ │
│  │  - 2GB Memory                                 │ │
│  │  - 2 vCPU                                     │ │
│  │  - 1-10 instances (auto-scaling)             │ │
│  │  - 900s timeout                               │ │
│  └───────────────────────────────────────────────┘ │
│                                                     │
│  Secrets (from Secret Manager):                    │
│  ├── research-env (.env file)                      │
│  └── gdrive-credentials (JSON)                     │
└─────────────────────────────────────────────────────┘
                        │
                        │ Connects to:
                        │
        ┌───────────────┼───────────────┐
        │               │               │
    ┌───▼────┐    ┌────▼─────┐   ┌────▼─────┐
    │MongoDB │    │ Airtable │   │ G Drive  │
    │        │    │   API    │   │   API    │
    └────────┘    └──────────┘   └──────────┘
```

## Prerequisites

### 1. Google Cloud Account
- Create account: https://cloud.google.com/free
- Get $300 free credit for new users
- Create a new project or use existing one

### 2. Install Google Cloud SDK
```bash
# macOS
brew install --cask google-cloud-sdk

# Linux
curl https://sdk.cloud.google.com | bash

# Windows
# Download installer from: https://cloud.google.com/sdk/docs/install
```

### 3. Authenticate
```bash
# Login to your Google account
gcloud auth login

# Set your project
gcloud config set project YOUR_PROJECT_ID

# Enable Application Default Credentials
gcloud auth application-default login
```

### 4. Prepare Configuration Files

#### a. `.env` file
Copy `.env.example` to `.env` and fill in all values:
```bash
cp .env.example .env
```

**Required environment variables:**
```bash
# API Keys
TAVILY_API_KEY=tvly-xxxxx
OPENAI_API_KEY=sk-xxxxx
ANTHROPIC_API_KEY=sk-ant-xxxxx
GEMINI_API_KEY=xxxxx

# Airtable
AIRTABLE_API_KEY=patxxxxx
AIRTABLE_BASE_ID=appxxxxx
AIRTABLE_TABLE_NAME=Companies

# Security (generate strong random keys)
JWT_SECRET_KEY=your-secure-random-string-here
API_KEY=your-api-key-here

# Optional: MongoDB
MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net/
MONGODB_DB_NAME=company_research

# Google Drive
EMAIL_TEMPLATES_FOLDER_ID=1tt4LLouNP2FgHcguIKlnRzRb3j5jE8LH

# Feature Flags
USE_MOCK_DATA=false
ENABLE_AIRTABLE_UPLOAD=true
ENABLE_GDRIVE_UPLOAD=true
```

#### b. `gdrive_credentials.json`
Get Google Drive service account credentials:

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Navigate to **APIs & Services** > **Credentials**
3. Create a **Service Account**
4. Generate and download JSON key
5. Save as `gdrive_credentials.json` in project root
6. Share your Google Drive folders with the service account email

## Deployment Steps

### Quick Deploy (Automated)

```bash
# Step 1: Run setup (first time only)
./deploy-gcloud.sh --setup --project-id YOUR_PROJECT_ID

# Step 2: Deploy
./deploy-gcloud.sh
```

### Manual Deploy (Step by Step)

#### 1. Enable Required APIs
```bash
gcloud services enable \
  run.googleapis.com \
  secretmanager.googleapis.com \
  cloudbuild.googleapis.com \
  containerregistry.googleapis.com
```

#### 2. Create Secrets in Secret Manager
```bash
# Store .env file
gcloud secrets create research-env --data-file=.env

# Store Google Drive credentials
gcloud secrets create gdrive-credentials --data-file=gdrive_credentials.json

# Verify secrets
gcloud secrets list
```

#### 3. Grant Secret Access to Cloud Run
```bash
# Get your project number
PROJECT_NUMBER=$(gcloud projects describe YOUR_PROJECT_ID --format="value(projectNumber)")

# Grant access
gcloud secrets add-iam-policy-binding research-env \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud secrets add-iam-policy-binding gdrive-credentials \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

#### 4. Deploy to Cloud Run
```bash
gcloud run deploy company-research-agent \
  --source . \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --timeout 900 \
  --concurrency 80 \
  --min-instances 1 \
  --max-instances 10 \
  --set-env-vars "PYTHONUNBUFFERED=1,PORT=8000,EMAIL_TEMPLATES_FOLDER_ID=1tt4LLouNP2FgHcguIKlnRzRb3j5jE8LH" \
  --update-secrets ".env=research-env:latest,/secrets/gdrive_credentials.json=gdrive-credentials:latest"
```

#### 5. Get Service URL
```bash
gcloud run services describe company-research-agent \
  --region us-central1 \
  --format 'value(status.url)'
```

Example output: `https://company-research-agent-xxxxx-uc.a.run.app`

## Configuration After Deployment

### 1. Update Airtable Extension

1. Open your Airtable base
2. Open the **Email Generator** extension
3. Click **Settings** (⚙️ icon)
4. Update **API Endpoint** to your Cloud Run URL:
   ```
   https://company-research-agent-xxxxx-uc.a.run.app
   ```
5. Click **Save**

### 2. Update Airtable Automations

For each automation that calls your webhook:

1. Edit the automation
2. Find the **Webhook** action
3. Replace the ngrok URL with Cloud Run URL:
   ```
   OLD: https://xxx.ngrok-free.app/webhook/start-research
   NEW: https://company-research-agent-xxxxx-uc.a.run.app/webhook/start-research
   ```
4. Save the automation

### 3. Test the Deployment

```bash
# Test health check
curl https://YOUR-SERVICE-URL/health

# Test templates endpoint
curl -H "ngrok-skip-browser-warning: true" \
  https://YOUR-SERVICE-URL/templates

# Test research webhook
curl -X POST https://YOUR-SERVICE-URL/webhook/start-research \
  -H "Content-Type: application/json" \
  -d '{
    "company": "Test Company",
    "airtable_record_id": "recXXXXXXXXXXXXXX",
    "google_drive_folder_url": "https://drive.google.com/drive/folders/YOUR_FOLDER_ID"
  }'
```

## Monitoring & Maintenance

### View Logs
```bash
# Stream real-time logs
gcloud run logs tail company-research-agent --region us-central1

# View recent logs
gcloud run logs read company-research-agent --region us-central1 --limit 100
```

### Monitor Metrics
1. Go to [Cloud Run Console](https://console.cloud.google.com/run)
2. Click on **company-research-agent**
3. View **METRICS** tab for:
   - Request count
   - Request latency
   - Instance count
   - CPU & Memory utilization
   - Billable container time

### Update Secrets
```bash
# Update environment variables
gcloud secrets versions add research-env --data-file=.env

# Update Google Drive credentials
gcloud secrets versions add gdrive-credentials --data-file=gdrive_credentials.json

# Redeploy service to use new secrets
gcloud run services update company-research-agent --region us-central1
```

### Update Code
```bash
# After making code changes, simply redeploy
gcloud run deploy company-research-agent \
  --source . \
  --region us-central1
```

### Scale Configuration
```bash
# Adjust min/max instances
gcloud run services update company-research-agent \
  --region us-central1 \
  --min-instances 2 \
  --max-instances 20

# Adjust memory/CPU
gcloud run services update company-research-agent \
  --region us-central1 \
  --memory 4Gi \
  --cpu 4

# Adjust timeout
gcloud run services update company-research-agent \
  --region us-central1 \
  --timeout 1200
```

## Cost Optimization

### Estimated Costs

**Cloud Run Pricing (us-central1):**
- **Free tier**: 2 million requests/month, 360,000 vCPU-seconds, 180,000 GiB-seconds
- **CPU**: $0.00002400/vCPU-second
- **Memory**: $0.00000250/GiB-second
- **Requests**: $0.40 per million requests

**Example Monthly Cost (100 research jobs/day):**
- 3,000 requests/month (~30 per job)
- ~1,500,000 vCPU-seconds (2 vCPU × ~8min avg job × 3,000 requests)
- ~1,500,000 GiB-seconds (2GB memory)

**Estimated: $40-60/month** (after free tier)

### Reduce Costs

1. **Use min-instances=0 for dev/testing**
   ```bash
   gcloud run services update company-research-agent \
     --region us-central1 \
     --min-instances 0
   ```
   ⚠️ Cold starts will be slower (~30-60 seconds)

2. **Lower memory for lighter workloads**
   ```bash
   gcloud run services update company-research-agent \
     --region us-central1 \
     --memory 1Gi
   ```

3. **Reduce timeout for faster failures**
   ```bash
   gcloud run services update company-research-agent \
     --region us-central1 \
     --timeout 300
   ```

4. **Set up billing alerts**
   - Go to [Billing](https://console.cloud.google.com/billing)
   - Create budget alert for your project
   - Set threshold (e.g., $50/month)

## Troubleshooting

### Deployment Fails

**Issue**: "Permission denied" error
```bash
# Grant Cloud Build permission
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:YOUR_PROJECT_NUMBER@cloudbuild.gserviceaccount.com" \
  --role="roles/run.admin"
```

**Issue**: "Secret not found"
```bash
# Verify secrets exist
gcloud secrets list

# Recreate if missing
gcloud secrets create research-env --data-file=.env
```

### Service Errors

**Issue**: 500 errors in logs
```bash
# Check logs for Python errors
gcloud run logs read company-research-agent --region us-central1

# Common causes:
# - Missing environment variables
# - Invalid API keys
# - Database connection issues
```

**Issue**: "Secret mount failed"
```bash
# Verify secret permissions
gcloud secrets get-iam-policy research-env
gcloud secrets get-iam-policy gdrive-credentials

# Re-grant access if needed
PROJECT_NUMBER=$(gcloud projects describe YOUR_PROJECT_ID --format="value(projectNumber)")
gcloud secrets add-iam-policy-binding research-env \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

### Performance Issues

**Issue**: Slow response times
- Increase CPU: `--cpu 4`
- Increase memory: `--memory 4Gi`
- Increase min-instances: `--min-instances 2`
- Check external API latency (Tavily, OpenAI, etc.)

**Issue**: Timeout errors
- Increase timeout: `--timeout 1200` (up to 3600s max)
- Optimize code for faster execution
- Use background tasks for long-running jobs

### Airtable Integration Issues

**Issue**: Extension can't reach service
- Verify URL is correct (no trailing slash)
- Check CORS configuration in `application.py`
- Ensure `--allow-unauthenticated` is set

**Issue**: Automations fail
- Check webhook URL format
- Verify JSON payload structure
- Check Cloud Run logs for error details

## Advanced Configuration

### Custom Domain

1. **Map custom domain:**
   ```bash
   gcloud run domain-mappings create --service company-research-agent \
     --domain api.yourdomain.com \
     --region us-central1
   ```

2. **Update DNS records** as instructed by gcloud

3. **Update Airtable** with new domain: `https://api.yourdomain.com`

### Authentication

Enable JWT authentication for public endpoints:

```bash
# Update to require authentication
gcloud run services update company-research-agent \
  --region us-central1 \
  --no-allow-unauthenticated

# Generate service account for Airtable
gcloud iam service-accounts create airtable-webhook \
  --display-name "Airtable Webhook Caller"

# Grant invoke permission
gcloud run services add-iam-policy-binding company-research-agent \
  --region us-central1 \
  --member="serviceAccount:airtable-webhook@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/run.invoker"
```

### CI/CD Pipeline

Use Cloud Build for automatic deployments:

```bash
# Connect GitHub repository
gcloud builds triggers create github \
  --repo-name=company-research-agent \
  --repo-owner=YOUR_GITHUB_USERNAME \
  --branch-pattern="^main$" \
  --build-config=cloudbuild.yaml

# Now every push to main branch auto-deploys!
```

## Security Best Practices

1. ✅ **Use Secret Manager** (not .env in container)
2. ✅ **Enable VPC** for database connections
3. ✅ **Rotate secrets** regularly (API keys)
4. ✅ **Set up IAM roles** (principle of least privilege)
5. ✅ **Enable Cloud Armor** for DDoS protection
6. ✅ **Monitor audit logs** for suspicious activity
7. ✅ **Use service accounts** with minimal permissions

## Support & Resources

- **Cloud Run Docs**: https://cloud.google.com/run/docs
- **Pricing Calculator**: https://cloud.google.com/products/calculator
- **Status Page**: https://status.cloud.google.com/
- **Support**: https://cloud.google.com/support

## Next Steps

After successful deployment:

1. ✅ Test all webhooks manually
2. ✅ Update Airtable extension & automations
3. ✅ Run a full research job end-to-end
4. ✅ Set up monitoring alerts
5. ✅ Configure billing budgets
6. ✅ Document your service URL
7. ✅ Set up CI/CD (optional)
8. ✅ Configure custom domain (optional)

**Congratulations! Your single Cloud Run service is now live with all webhooks accessible! 🎉**
