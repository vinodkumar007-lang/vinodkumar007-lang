2025-07-10T16:59:19.744+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 🪪 OT JobId: cc7f8f82-aafe-4418-b01a-8c45e94598ce, InstanceId: 2c93525b-42d1-410a-9e26-aa957f19861d
2025-07-10T16:59:19.745+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Waiting for _STDDELIVERYFILE.xml in: /mnt/nfs/dev-exstream/dev-SA/jobs/cc7f8f82-aafe-4418-b01a-8c45e94598ce/2c93525b-42d1-410a-9e26-aa957f19861d/docgen
2025-07-10T16:59:19.853+02:00 ERROR 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Failed in processing

java.nio.file.NoSuchFileException: /mnt/nfs/dev-exstream/dev-SA/jobs/cc7f8f82-aafe-4418-b01a-8c45e94598ce/2c93525b-42d1-410a-9e26-aa957f19861d/docgen
	at java.base/sun.nio.fs.UnixException.translateToIOException(UnixException.java:92) ~[na:na]
	at java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:106) ~[na:na]
	at java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:111) ~[na:na]
