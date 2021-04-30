# Using flows
Viadot flows subclass Prefect Flow class. We take this class, specify the tasks, and build the flow using the parameters provided at initialization time. See [Prefect Flow documentation](https://docs.prefect.io/api/0.12.6/core/flow.html#flow-2) for more information. 

For instance, a `S3 to Redshift` flow would include the tasks necessary to insert S3 files into Redshift, and automate the generation of the flow. You only need to pass the required parameters.

For examples, check out the `examples` folder.


# Writing flows
For now, see the existing flows in `viadot/flows`.