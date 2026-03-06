from django.contrib import admin
from .models import GHLLocation, GHLOpportunity


@admin.register(GHLLocation)
class GHLLocationAdmin(admin.ModelAdmin):
    list_display = ('location_id', 'company_name', 'status', 'onboarded_at', 'created_at')
    list_filter = ('status',)
    search_fields = ('location_id', 'company_name')


@admin.register(GHLOpportunity)
class GHLOpportunityAdmin(admin.ModelAdmin):
    list_display = ('opportunity_id', 'location', 'created_at', 'updated_at')
    list_filter = ('location',)
    search_fields = ('opportunity_id', 'raw_data__name')
    raw_id_fields = ('location',)
