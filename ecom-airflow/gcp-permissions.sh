# Create PubSub topic and subscription
gcloud pubsub topics create gcs-products-topic
gcloud pubsub subscriptions create gcs-products-sub --topic=gcs-products-topic
gcloud pubsub topics create gcs-users-topic
gcloud pubsub subscriptions create gcs-users-sub --topic=gcs-users-topic
gcloud pubsub topics create gcs-orders-topic
gcloud pubsub subscriptions create gcs-orders-sub --topic=gcs-orders-topic
gcloud pubsub topics create gcs-order_items-topic
gcloud pubsub subscriptions create gcs-order_items-sub --topic=gcs-order_items-topic

# GCS to publish events
gcloud storage buckets notifications create gs://synthetic-ecom-data \
    --topic=gcs-products-topic \
    --event-types=OBJECT_FINALIZE \
    --payload-format=json \
    --object-prefix=products/
gcloud storage buckets notifications create gs://synthetic-ecom-data \
    --topic=gcs-users-topic \
    --event-types=OBJECT_FINALIZE
    --payload-format=json \
    --object-prefix=users/
gcloud storage buckets notifications create gs://synthetic-ecom-data \
    --topic=gcs-orders-topic \
    --event-types=OBJECT_FINALIZE \
    --payload-format=json \
    --object-prefix=orders/
gcloud storage buckets notifications create gs://synthetic-ecom-data \
    --topic=gcs-order_items-topic \
    --event-types=OBJECT_FINALIZE \
    --payload-format=json \
    --object-prefix=order_items/

# Service account setup:
gcloud iam service-accounts create astro-pubsub-reader \
    --description="Service account for Astro Airflow to pull GCS events" \
    --display-name="Astro PubSub Reader"
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
    --member="serviceAccount:astro-pubsub-reader@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/pubsub.subscriber"

# Permission for GCS services agent
PROJECT_NUMBER=$(gcloud projects describe $GCP_PROJECT_ID --format='value(projectNumber)')
gcloud pubsub topics add-iam-policy-binding gcs-events \
    --member="serviceAccount:service-${PROJECT_NUMBER}@gs-project-accounts.iam.gserviceaccount.com" \
    --role="roles/pubsub.publisher"

# To check subs or delete
# gcloud storage buckets notifications list gs://synthetic-ecom-data
# gcloud storage buckets notifications delete gs://synthetic-ecom-data


# Create key for astro
gcloud iam service-accounts keys create ./include/gcp-key.json \
    --iam-account=astro-pubsub-reader@${GCP_PROJECT_ID}.iam.gserviceaccount.com