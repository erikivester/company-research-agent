# Email Templates Guide

This guide provides information about creating and managing email templates for the outreach email generator.

## Template Format

Email templates can be created in several formats:
- Plain text (.txt)
- Markdown (.md)
- Google Docs

## Template Structure

Each template should follow this structure:

1. First line: Brief description of the template's purpose
2. Subject line (starting with "Subject:")
3. Greeting with placeholder
4. Body content with placeholders
5. Closing

### Example Template

```
Template for engaging sustainability-focused prospects in the energy sector

Subject: Advancing {company}'s sustainability initiatives

Dear {name},

I noticed {company}'s impressive work on {focus_area} and wanted to reach out. Given your role as {title} and your company's commitment to {specific_initiative}, I believe our solution could provide significant value.

[Body content with personalized insights]

Would you be open to a brief discussion about how we could support your initiatives?

Best regards,
[Your name]
```

## Placeholders

Templates can include these placeholders:

- `{name}`: Contact's full name
- `{company}`: Company name
- `{title}`: Contact's title
- `{focus_area}`: Main area of focus from research
- `{specific_initiative}`: Specific initiative mentioned in research
- `{industry}`: Company's industry

## Best Practices

1. Keep templates concise and focused
2. Use professional language
3. Include space for personalization
4. Avoid industry jargon unless appropriate
5. Test templates with various inputs

## File Naming

Follow these naming conventions:
- Use UPPERCASE for template types
- Replace spaces with underscores
- Be descriptive but concise
- Example: `CGF_METHANE_CALL.txt`

## Template Categories

Organize templates by purpose:
1. Initial Outreach
2. Follow-up
3. Industry-specific
4. Initiative-specific

## Testing Templates

Before using a new template:
1. Test with sample data
2. Review AI-generated outputs
3. Check placeholder replacements
4. Verify formatting

## Updating Templates

1. Make a backup of the existing template
2. Update the content
3. Test thoroughly
4. Update the template description if needed

## Template Storage

Templates are stored in a shared Google Drive folder:
1. Must be accessible to the service account
2. Keep backups of critical templates
3. Maintain version history

## Common Issues

1. Missing placeholders
   - Solution: Review template against placeholder list

2. Formatting problems
   - Solution: Check line breaks and spacing

3. Too generic
   - Solution: Add more specific placeholder points

4. Too specific
   - Solution: Make content more adaptable

## Template Review Process

1. Initial draft
2. Peer review
3. Test with AI generation
4. Revise based on output
5. Final approval
6. Deploy to production

## Contact

For template-related questions or issues:
- Email: support@example.com
- Documentation: [API.md](API.md)