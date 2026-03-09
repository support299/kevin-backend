from django.urls import path

from . import views

urlpatterns = [
    path('opportunities/filters/', views.OpportunityFiltersView.as_view(), name='ghl-opportunities-filters'),
    path('opportunities/', views.OpportunityListView.as_view(), name='ghl-opportunities'),
    path('pipelines/', views.PipelinesListView.as_view(), name='ghl-pipelines'),
    path('onboard/', views.GHLOnboardView.as_view(), name='ghl-onboard'),
    path('oauth/authorize/', views.GHLOAuthAuthorizeView.as_view(), name='ghl-oauth-authorize'),
    path('oauth/callback/', views.GHLOAuthCallbackView.as_view(), name='ghl-oauth-callback'),
    path('webhooks/opportunity/', views.GHLWebhookView.as_view(), name='ghl-webhook-opportunity'),
]
