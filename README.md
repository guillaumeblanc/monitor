![image](https://github.com/guillaumeblanc/monitor/assets/7301602/73b9c469-fb59-4fe8-b279-f066e82eae34)Monitoring prototype.

Provides continuous tracking of plants.
Only supports Huawei (Fusion Solar) at the moment. More providers to come as needed.

# Setup

## Google Service Account

Collected and aggregated data are stored on Google drive, which requires a Google Service Account. It is required to create a machine-to-machine communication (no user authentication).

Here's a procedure example: https://blog.zephyrok.com/google-drive-api-with-service-account-in-python/

# Dependencies
- Huawei Fusion Solar Northbound interface client: [pyhfs](https://github.com/guillaumeblanc/pyhfs/)
- pydrive
