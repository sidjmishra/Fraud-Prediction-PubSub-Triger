from google.cloud import aiplatform as vertex_ai
import numpy as np

def inferencing_record(record):
    project = 'idea-ind-data-ai-dev'
    region = "us-central1"
    endpoint_display_name = 'fraud_detect_model_ep1'

    vertex_ai.init(project=project, location=region)

    filter = f'display_name="{endpoint_display_name}"'
    # Searching for the mentioned endpoint name
    for endpoint_info in vertex_ai.Endpoint.list(filter=filter):
        print(f"Endpoint display name = {endpoint_info.display_name} resource id ={endpoint_info.resource_name}")
    # Intialising the endpoint
    endpoint = vertex_ai.Endpoint(endpoint_info.resource_name)
    print(endpoint)

    defect_classfication = endpoint.predict(record)
    
    return defect_classfication.predictions