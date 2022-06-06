## Testing Prefect 2.0
This branch contains flows used for testing the new functionalities of Prefect 2.0.

## How to run the flow
```bash
docker exec -it orion_lab bash
cd notebooks/orion/prefect
python -m flows.test_platform_flow.test_platform_flow
```
