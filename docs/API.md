# AI-Powered Outreach Email Generator API Documentation

This API provides endpoints for generating highly personalized outreach emails by combining email templates with research context and Airtable data.

## Authentication

The API uses JWT (JSON Web Token) authentication. Include the token in the Authorization header:

```
Authorization: Bearer your-jwt-token
```

## Rate Limiting

The API implements rate limiting to ensure fair usage:
- Default limit: 100 requests per hour
- Limits can be configured via environment variables

## Endpoints

### 1. List Email Templates
```http
GET /templates
```

Lists all available email templates with their descriptions.

#### Response
```json
{
  "CGF_METHANE_CALL": "Template for methane reduction initiatives",
  "SUSTAINABILITY_INTRO": "Introduction template for sustainability prospects"
}
```

### 2. Generate Outreach Email
```http
POST /generate-outreach
```

Generates a personalized outreach email by combining template, research, and Airtable context.

#### Request Body
```json
{
  "airtable_context": {
    "name": "Company Name",
    "title": "Contact Title",
    "summary": "Company Summary",
    "angle_for_outreach": "Strategic Notes",
    "note": "Additional Context"
  },
  "contact_name": "John Doe",
  "google_drive_folder_url": "https://drive.google.com/folders/folder-id",
  "template_type": "TEMPLATE_NAME"
}
```

#### Response
```json
{
  "email_text": "Generated email content...",
  "template_used": "Template Name",
  "context_used": {
    "template": true,
    "research": true,
    "airtable": true
  }
}
```

## Template Requirements

### Template Format
Email templates should be stored in the designated Google Drive folder and follow these guidelines:

1. File Format:
   - Plain text (.txt)
   - Markdown (.md)
   - Google Docs

2. Naming Convention:
   - Use descriptive names
   - No spaces (use underscores)
   - Example: `CGF_METHANE_CALL.txt`

3. Template Structure:
   - First line: Brief description
   - Include placeholders for dynamic content: `{name}`, `{company}`, etc.
   - Clear sections for:
     - Subject line
     - Greeting
     - Body
     - Closing

### Example Template
```
Template for engaging sustainability-focused prospects

Subject: Advancing {company}'s sustainability initiatives

Dear {name},

I noticed {company}'s impressive work on {focus_area}...

[Body content]

Best regards,
[Your name]
```

## Research Context

The API expects research context to be available in the specified Google Drive folder. Supported file types:

- JSON files (for structured data)
- Text files
- Markdown files
- PDF files

### Research File Structure
For optimal results, research files should be organized as:

1. Company Brief (JSON):
```json
{
  "company_brief_data": {
    "url1": {
      "title": "Article Title",
      "content": "Article content..."
    }
  }
}
```

2. Notes (Text/Markdown):
- Bullet points of key insights
- Recent developments
- Strategic considerations

## Error Handling

The API uses standard HTTP status codes and provides detailed error messages:

- 400: Bad Request (invalid input)
- 401: Unauthorized (invalid/missing token)
- 404: Not Found (template/resource not found)
- 429: Too Many Requests (rate limit exceeded)
- 500: Internal Server Error

### Error Response Format
```json
{
  "detail": "Error description"
}
```

## Best Practices

1. Template Management:
   - Keep templates concise and focused
   - Update templates regularly
   - Test templates before using in production

2. Research Context:
   - Ensure research files are up-to-date
   - Organize content logically
   - Include relevant, recent information

3. API Usage:
   - Implement proper error handling
   - Respect rate limits
   - Cache responses when appropriate

## Environment Variables

Configure the API using these environment variables:

```
JWT_SECRET_KEY=your-secret-key
ALLOWED_ORIGINS=https://your-domain.com
RATE_LIMIT_REQUESTS=100
RATE_LIMIT_PERIOD=3600
GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json
```

## Development and Testing

1. Local Setup:
```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export JWT_SECRET_KEY=your-secret-key
export GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json

# Run the server
uvicorn application:app --reload
```

2. Running Tests:
```bash
pytest tests/
```

## Examples

### Python Client Example
```python
import requests

def generate_email(api_key, template_type, contact_name, airtable_context, folder_url):
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    data = {
        "template_type": template_type,
        "contact_name": contact_name,
        "airtable_context": airtable_context,
        "google_drive_folder_url": folder_url
    }
    
    response = requests.post(
        "https://api.example.com/generate-outreach",
        json=data,
        headers=headers
    )
    
    return response.json()
```

### cURL Example
```bash
curl -X POST "https://api.example.com/generate-outreach" \
  -H "Authorization: Bearer your-jwt-token" \
  -H "Content-Type: application/json" \
  -d '{
    "template_type": "CGF_METHANE_CALL",
    "contact_name": "John Doe",
    "airtable_context": {
      "name": "Example Corp",
      "title": "Sustainability Director",
      "summary": "Leading energy company",
      "angle_for_outreach": "Recent commitments",
      "note": "Interested in technology"
    },
    "google_drive_folder_url": "https://drive.google.com/folders/folder-id"
  }'
```