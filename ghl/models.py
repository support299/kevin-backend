from django.db import models
from django.utils import timezone


class GHLLocation(models.Model):
    """GHL sub-account (location) with OAuth tokens."""
    location_id = models.CharField(max_length=100, unique=True)
    company_name = models.CharField(max_length=255, blank=True)
    status = models.CharField(max_length=50, default='active', blank=True)
    webhook_url = models.URLField(blank=True)
    webhook_secret = models.CharField(max_length=255, blank=True)
    # OAuth tokens
    access_token = models.TextField(blank=True, help_text="OAuth access token")
    refresh_token = models.TextField(blank=True, help_text="OAuth refresh token")
    token_expires_at = models.DateTimeField(null=True, blank=True, help_text="When the access token expires")
    metadata = models.JSONField(default=dict, blank=True)
    onboarded_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.company_name or 'GHL Location'} ({self.location_id})"

    def is_token_valid(self):
        """Check if the access token is still valid."""
        if not self.access_token or not self.token_expires_at:
            return False
        return timezone.now() < self.token_expires_at

    def needs_token_refresh(self):
        """Check if token needs to be refreshed (within 5 minutes of expiry)."""
        if not self.token_expires_at:
            return True
        return timezone.now() >= (self.token_expires_at - timezone.timedelta(minutes=5))


class GHLOpportunity(models.Model):
    """Cached full opportunity from GHL (fetched via API after webhook)."""
    opportunity_id = models.CharField(max_length=100, unique=True)
    location = models.ForeignKey(GHLLocation, on_delete=models.CASCADE, related_name='opportunities')
    # Full raw payload from GHL API (GET /opportunities/:id)
    raw_data = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-updated_at']
        verbose_name_plural = 'GHL opportunities'

    def __str__(self):
        name = self.raw_data.get('name') or self.opportunity_id
        return f"{name} ({self.opportunity_id})"


class OpportunityReport(models.Model):
    """
    Report table synced from GHL opportunities (HMG pipeline).
    Updated whenever an opportunity is created/updated via webhook.
    Uses db_table='opportunity_report' - if table exists with different schema, set managed=False.
    """
    opportunity_id = models.CharField(max_length=100, unique=True, primary_key=True)
    location_id = models.CharField(max_length=100)
    location_name = models.CharField(max_length=255, blank=True)
    contact_name = models.CharField(max_length=255, blank=True)
    contact_email = models.CharField(max_length=255, blank=True)
    contact_phone = models.CharField(max_length=100, blank=True)
    name = models.CharField(max_length=255, blank=True)
    status = models.CharField(max_length=50, blank=True)
    monetary_value = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    contact_id = models.CharField(max_length=100, blank=True)
    pipeline_id = models.CharField(max_length=100, blank=True)
    pipeline_stage_id = models.CharField(max_length=100, blank=True)
    date_added = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'opportunity_report'
        ordering = ['-updated_at']
        verbose_name_plural = 'Opportunity reports'

    def __str__(self):
        return f"{self.contact_name or self.opportunity_id} ({self.opportunity_id})"
