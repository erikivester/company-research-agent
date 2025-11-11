#!/bin/bash

# =============================================================================
# Google Cloud Run Deployment Script
# =============================================================================
# This script deploys the company-research-agent to Google Cloud Run
# 
# Prerequisites:
# 1. Google Cloud SDK installed: https://cloud.google.com/sdk/docs/install
# 2. Authenticated: gcloud auth login
# 3. Project selected: gcloud config set project YOUR_PROJECT_ID
# 4. Secrets created (see SECRETS_SETUP.md)
# 
# Usage:
#   ./deploy-gcloud.sh [OPTIONS]
# 
# Options:
#   --project-id    Your GCP project ID (required on first run)
#   --region        Deployment region (default: us-central1)
#   --service-name  Cloud Run service name (default: company-research-agent)
#   --setup         Run initial setup (secrets, APIs)
# 
# Examples:
#   ./deploy-gcloud.sh --project-id my-project-123
#   ./deploy-gcloud.sh --setup
#   ./deploy-gcloud.sh --region us-east1
# =============================================================================

set -e  # Exit on any error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
REGION="us-central1"
SERVICE_NAME="company-research-agent"
MEMORY="2Gi"
CPU="2"
TIMEOUT="1200"
MIN_INSTANCES="1"
MAX_INSTANCES="10"
CONCURRENCY="80"

# Parse command line arguments
SETUP_MODE=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --project-id)
            PROJECT_ID="$2"
            shift 2
            ;;
        --region)
            REGION="$2"
            shift 2
            ;;
        --service-name)
            SERVICE_NAME="$2"
            shift 2
            ;;
        --setup)
            SETUP_MODE=true
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Company Research Agent - Cloud Run Deployment       ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════╝${NC}"
echo ""

# Get project ID from gcloud if not provided
if [ -z "$PROJECT_ID" ]; then
    PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
    if [ -z "$PROJECT_ID" ]; then
        echo -e "${RED}❌ Error: No project ID specified${NC}"
        echo -e "Please set it with: ${YELLOW}gcloud config set project YOUR_PROJECT_ID${NC}"
        echo -e "Or use: ${YELLOW}./deploy-gcloud.sh --project-id YOUR_PROJECT_ID${NC}"
        exit 1
    fi
fi

echo -e "${GREEN}✓${NC} Using project: ${YELLOW}$PROJECT_ID${NC}"
echo -e "${GREEN}✓${NC} Region: ${YELLOW}$REGION${NC}"
echo -e "${GREEN}✓${NC} Service name: ${YELLOW}$SERVICE_NAME${NC}"
echo ""

# Setup mode: Enable APIs and create secrets
if [ "$SETUP_MODE" = true ]; then
    echo -e "${BLUE}🔧 Running initial setup...${NC}"
    
    # Enable required APIs
    echo -e "${YELLOW}Enabling Google Cloud APIs...${NC}"
    gcloud services enable run.googleapis.com \
        secretmanager.googleapis.com \
        cloudbuild.googleapis.com \
        containerregistry.googleapis.com \
        --project=$PROJECT_ID
    
    echo -e "${GREEN}✓${NC} APIs enabled"
    
    # Check if .env file exists
    if [ ! -f ".env" ]; then
        echo -e "${RED}❌ Error: .env file not found${NC}"
        echo -e "Please create .env file from .env.example and fill in your values"
        exit 1
    fi
    
    # Create secrets
    echo -e "${YELLOW}Creating secrets in Secret Manager...${NC}"
    
    # Create environment variables secret
    if gcloud secrets describe research-env --project=$PROJECT_ID >/dev/null 2>&1; then
        echo -e "${YELLOW}Secret 'research-env' already exists, updating...${NC}"
        gcloud secrets versions add research-env --data-file=.env --project=$PROJECT_ID
    else
        gcloud secrets create research-env --data-file=.env --project=$PROJECT_ID
    fi
    echo -e "${GREEN}✓${NC} Created/updated secret: research-env"
    
    # Create Google Drive credentials secret
    if [ -f "gdrive_credentials.json" ]; then
        if gcloud secrets describe gdrive-credentials --project=$PROJECT_ID >/dev/null 2>&1; then
            echo -e "${YELLOW}Secret 'gdrive-credentials' already exists, updating...${NC}"
            gcloud secrets versions add gdrive-credentials --data-file=gdrive_credentials.json --project=$PROJECT_ID
        else
            gcloud secrets create gdrive-credentials --data-file=gdrive_credentials.json --project=$PROJECT_ID
        fi
        echo -e "${GREEN}✓${NC} Created/updated secret: gdrive-credentials"
    else
        echo -e "${YELLOW}⚠${NC}  gdrive_credentials.json not found, skipping Google Drive secret"
    fi
    
    echo ""
    echo -e "${GREEN}✓ Setup complete!${NC}"
    echo ""
fi

# Confirm deployment
echo -e "${YELLOW}Ready to deploy to Cloud Run${NC}"
echo -e "This will:"
echo -e "  • Build Docker image from Dockerfile"
echo -e "  • Push to Google Container Registry"
echo -e "  • Deploy to Cloud Run with:"
echo -e "    - Memory: $MEMORY"
echo -e "    - CPU: $CPU"
echo -e "    - Timeout: ${TIMEOUT}s"
echo -e "    - Min instances: $MIN_INSTANCES"
echo -e "    - Max instances: $MAX_INSTANCES"
echo ""
read -p "Continue? (y/N) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Deployment cancelled${NC}"
    exit 0
fi

echo ""
echo -e "${BLUE}🚀 Starting deployment...${NC}"
echo ""

# Deploy to Cloud Run
gcloud run deploy $SERVICE_NAME \
    --source . \
    --platform managed \
    --region $REGION \
    --project $PROJECT_ID \
    --allow-unauthenticated \
    --memory $MEMORY \
    --cpu $CPU \
    --timeout $TIMEOUT \
    --concurrency $CONCURRENCY \
    --min-instances $MIN_INSTANCES \
    --max-instances $MAX_INSTANCES \
    --set-env-vars "PYTHONUNBUFFERED=1,EMAIL_TEMPLATES_FOLDER_ID=1tt4LLouNP2FgHcguIKlnRzRb3j5jE8LH" \
    --update-secrets ".env=research-env:latest,/secrets/gdrive_credentials.json=gdrive-credentials:latest"

echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║              Deployment Successful! 🎉                 ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════╝${NC}"
echo ""

# Get the service URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region $REGION --project $PROJECT_ID --format 'value(status.url)')

echo -e "${BLUE}Service URL:${NC} ${YELLOW}$SERVICE_URL${NC}"
echo ""
echo -e "${BLUE}Available webhooks:${NC}"
echo -e "  • Research:      ${YELLOW}$SERVICE_URL/webhook/start-research${NC}"
echo -e "  • Email Gen:     ${YELLOW}$SERVICE_URL/generate-outreach${NC}"
echo -e "  • PDF Gen:       ${YELLOW}$SERVICE_URL/generate-pdf${NC}"
echo -e "  • Templates:     ${YELLOW}$SERVICE_URL/templates${NC}"
echo -e "  • Health Check:  ${YELLOW}$SERVICE_URL/health${NC}"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo -e "  1. Test the health endpoint:"
echo -e "     ${YELLOW}curl $SERVICE_URL/health${NC}"
echo ""
echo -e "  2. Update Airtable extension settings:"
echo -e "     • Open your Airtable base"
echo -e "     • Open the Email Generator extension"
echo -e "     • Click Settings (⚙️)"
echo -e "     • Set API Endpoint to: ${YELLOW}$SERVICE_URL${NC}"
echo ""
echo -e "  3. Update Airtable automations:"
echo -e "     • Edit webhook actions"
echo -e "     • Replace ngrok URL with: ${YELLOW}$SERVICE_URL${NC}"
echo ""
echo -e "  4. Monitor logs:"
echo -e "     ${YELLOW}gcloud run logs tail $SERVICE_NAME --region $REGION --project $PROJECT_ID${NC}"
echo ""
echo -e "${GREEN}Deployment complete! 🚀${NC}"
