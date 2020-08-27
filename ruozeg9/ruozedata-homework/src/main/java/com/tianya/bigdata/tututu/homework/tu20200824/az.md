```$xslt
For the most flexibility, one can always build new types by extending the existing ones. Azkaban uses reflection to load job types that implements the job interface, and tries to construct a sample object upon loading for basic testing. When executing a real job, Azkaban calls the run method to run the job, and cancel method to cancel it.

For new hadoop job types, it is important to use the correct hadoopsecuritymanager class, which is also included in azkaban-plugins repo. This class handles talking to the hadoop cluster, and if needed, requests tokens for job execution or for name node communication.

For better security, tokens should be requested in Azkaban main process and be written to a file. Before executing user code, the job type should implement a wrapper that picks up the token file, set it in the Configuration or JobConf object. Please refer to HadoopJavaJob and HadoopPigJob to see example usage.

```