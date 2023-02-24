import logging

import azure.functions as func
import azure.durable_functions as df
import lillorgid.etl.logging

lillorgid.etl.logging.log_to_azure()

async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    lillorgid.etl.logging.logger.info("Function function_start_from_http called")
    client = df.DurableOrchestrationClient(starter)
    await client.start_new(
        orchestration_function_name=req.route_params['functionName'],
        client_input={})
    return "Started"
