# Quick Reference: Common GCloud Commands

## Deployment Commands

```bash
# Quick deploy (after initial setup)
./deploy-gcloud.sh

# Deploy with custom region
./deploy-gcloud.sh --region us-east1

# First-time setup
./deploy-gcloud.sh --setup --project-id YOUR_PROJECT_ID
```

## Managing Secrets

```bash
# List all secrets
gcloud secrets list

# View secret metadata
gcloud secrets describe research-env

# Update .env secret
gcloud secrets versions add research-env --data-file=.env

# Update Google Drive credentials
gcloud secrets versions add gdrive-credentials --data-file=gdrive_credentials.json

# View secret versions
gcloud secrets versions list research-env

# Access secret value (for debugging)
gcloud secrets versions access latest --secret=research-env
```

## Service Management

```bash
# Get service URL
gcloud run services describe company-research-agent \
  --region us-central1 \
  --format 'value(status.url)'

# Update service (after secret change)
gcloud run services update company-research-agent --region us-central1

# Scale up/down
gcloud run services update company-research-agent \
  --region us-central1 \
  --min-instances 0 \
  --max-instances 20

# Adjust resources
gcloud run services update company-research-agent \
  --region us-central1 \
  --memory 4Gi \
  --cpu 4

# Change timeout
gcloud run services update company-research-agent \
  --region us-central1 \
  --timeout 1200

# Delete service
gcloud run services delete company-research-agent --region us-central1
```

## Monitoring

```bash
# Stream logs (real-time)
gcloud run logs tail company-research-agent --region us-central1

# Read recent logs
gcloud run logs read company-research-agent \
  --region us-central1 \
  --limit 100

# Filter logs by severity
gcloud run logs read company-research-agent \
  --region us-central1 \
  --log-filter="severity>=ERROR"

# Open Cloud Console
gcloud run services describe company-research-agent \
  --region us-central1 \
  --format 'value(metadata.selfLink)'
```

## Testing Endpoints

```bash
# Save service URL to variable
export SERVICE_URL=$(gcloud run services describe company-research-agent --region us-central1 --format 'value(status.url)')

# Test health endpoint
curl $SERVICE_URL/health

# Test templates endpoint
curl -H "ngrok-skip-browser-warning: true" $SERVICE_URL/templates | jq

# Test research webhook
curl -X POST $SERVICE_URL/webhook/start-research \
  -H "Content-Type: application/json" \
  -d '{
    "company": "Test Company",
    "airtable_record_id": "recTEST123",
    "google_drive_folder_url": "https://drive.google.com/drive/folders/YOUR_FOLDER_ID",
    "use_local_context": true
  }'

# Test email generation
curl -X POST $SERVICE_URL/generate-outreach \
  -H "Content-Type: application/json" \
  -d '{
    "template_type": "TEMPLATE_NAME",
    "contact_name": "John Doe",
    "airtable_context": {
      "name": "Test Company",
      "title": "CEO"
    }
  }' | jq
```

## Project Configuration

```bash
# List projects
gcloud projects list

# Set active project
gcloud config set project YOUR_PROJECT_ID

# Get current project
gcloud config get-value project

# List enabled APIs
gcloud services list --enabled
```

## IAM & Permissions

```bash
# Get project number
gcloud projects describe YOUR_PROJECT_ID --format="value(projectNumber)"

# Grant secret access to Cloud Run
PROJECT_NUMBER=$(gcloud projects describe $(gcloud config get-value project) --format="value(projectNumber)")

gcloud secrets add-iam-policy-binding research-env \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

# List IAM bindings for a secret
gcloud secrets get-iam-policy research-env
```

## Cost Management

```bash
# Estimate current costs
gcloud alpha billing accounts list

# View project billing
gcloud alpha billing projects describe YOUR_PROJECT_ID

# Set up budget alert (via Console)
# Go to: https://console.cloud.google.com/billing/budgets
```

## Troubleshooting

```bash
# Check service status
gcloud run services describe company-research-agent \
  --region us-central1 \
  --format 'value(status.conditions)'

# Get recent revisions
gcloud run revisions list --service company-research-agent --region us-central1

# Check last deployment
gcloud run revisions describe REVISION_NAME --region us-central1

# View build history
gcloud builds list --limit 10

# Check quota usage
gcloud compute project-info describe --project YOUR_PROJECT_ID
```

## Rollback

```bash
# List revisions
gcloud run revisions list --service company-research-agent --region us-central1

# Rollback to previous revision
gcloud run services update-traffic company-research-agent \
  --region us-central1 \
  --to-revisions PREVIOUS_REVISION=100
```

## Cleanup

```bash
# Delete service
gcloud run services delete company-research-agent --region us-central1

# Delete secrets
gcloud secrets delete research-env
gcloud secrets delete gdrive-credentials

# Delete container images
gcloud container images list
gcloud container images delete gcr.io/YOUR_PROJECT_ID/company-research-agent:TAG
```

## Useful Environment Variables

Add to your `~/.zshrc` or `~/.bashrc`:

```bash
# Set default project
export GCP_PROJECT_ID="your-project-id"
export GCP_REGION="us-central1"
export SERVICE_NAME="company-research-agent"

# Quick aliases
alias gcp-logs='gcloud run logs tail $SERVICE_NAME --region $GCP_REGION'
alias gcp-url='gcloud run services describe $SERVICE_NAME --region $GCP_REGION --format "value(status.url)"'
alias gcp-deploy='gcloud run deploy $SERVICE_NAME --source . --region $GCP_REGION'
```

## Emergency Commands

```bash
# Stop all traffic (set min-instances to 0)
gcloud run services update company-research-agent \
  --region us-central1 \
  --min-instances 0 \
  --max-instances 0

# Re-enable (restore scaling)
gcloud run services update company-research-agent \
  --region us-central1 \
  --min-instances 1 \
  --max-instances 10

# Force new deployment (rebuild container)
gcloud run deploy company-research-agent \
  --source . \
  --region us-central1 \
  --no-traffic  # Deploy without sending traffic

# Then gradually shift traffic
gcloud run services update-traffic company-research-agent \
  --region us-central1 \
  --to-revisions LATEST=10,PREVIOUS=90

# Full traffic to new version
gcloud run services update-traffic company-research-agent \
  --region us-central1 \
  --to-latest
```
