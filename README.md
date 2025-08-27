2025-08-27T07:54:02.024+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.BlobStorageService       : 📤 Uploaded file to 'https://nsnetextr01.blob.core.windows.net/nsneteextrm/DEBTMAN%2F76868555-98c9-4298-8e40-879d85b1ba8a%2F19ef9d68-b114-4803-b09b-ncdnc7-c8c6-d6cs%2Farchive%2F5898460768094732_EML001.pdf'
2025-08-27T07:54:02.024+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [76868555-98c9-4298-8e40-879d85b1ba8a] 📦 Uploaded archive file for account 5898460768094732: https://nsnetextr01.blob.core.windows.net/nsneteextrm/DEBTMAN/76868555-98c9-4298-8e40-879d85b1ba8a/19ef9d68-b114-4803-b09b-ncdnc7-c8c6-d6cs/archive/5898460768094732_EML001.pdf
2025-08-27T07:54:02.145+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.BlobStorageService       : 📤 Uploaded file to 'https://nsnetextr01.blob.core.windows.net/nsneteextrm/DEBTMAN%2F76868555-98c9-4298-8e40-879d85b1ba8a%2F19ef9d68-b114-4803-b09b-ncdnc7-c8c6-d6cs%2Farchive%2F5898460768094732_EML002.pdf'
2025-08-27T07:54:02.145+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [76868555-98c9-4298-8e40-879d85b1ba8a] 📦 Uploaded archive file for account 5898460768094732: https://nsnetextr01.blob.core.windows.net/nsneteextrm/DEBTMAN/76868555-98c9-4298-8e40-879d85b1ba8a/19ef9d68-b114-4803-b09b-ncdnc7-c8c6-d6cs/archive/5898460768094732_EML002.pdf
2025-08-27T07:54:02.299+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.BlobStorageService       : 📤 Uploaded file to 'https://nsnetextr01.blob.core.windows.net/nsneteextrm/DEBTMAN%2F76868555-98c9-4298-8e40-879d85b1ba8a%2F19ef9d68-b114-4803-b09b-ncdnc7-c8c6-d6cs%2Farchive%2F5898460768094732_EML003.pdf'
2025-08-27T07:54:02.299+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [76868555-98c9-4298-8e40-879d85b1ba8a] 📦 Uploaded archive file for account 5898460768094732: https://nsnetextr01.blob.core.windows.net/nsneteextrm/DEBTMAN/76868555-98c9-4298-8e40-879d85b1ba8a/19ef9d68-b114-4803-b09b-ncdnc7-c8c6-d6cs/archive/5898460768094732_EML003.pdf
2025-08-27T07:54:02.406+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.BlobStorageService       : 📤 Uploaded file to 'https://nsnetextr01.blob.core.windows.net/nsneteextrm/DEBTMAN%2F76868555-98c9-4298-8e40-879d85b1ba8a%2F19ef9d68-b114-4803-b09b-ncdnc7-c8c6-d6cs%2Farchive%2F5898460768094732_EMLCCAAA.pdf'
2025-08-27T07:54:02.406+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [76868555-98c9-4298-8e40-879d85b1ba8a] 📦 Uploaded archive file for account 5898460768094732: https://nsnetextr01.blob.core.windows.net/nsneteextrm/DEBTMAN/76868555-98c9-4298-8e40-879d85b1ba8a/19ef9d68-b114-4803-b09b-ncdnc7-c8c6-d6cs/archive/5898460768094732_EMLCCAAA.pdf
2025-08-27T07:54:02.568+02:00 ERROR 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [76868555-98c9-4298-8e40-879d85b1ba8a] ❌ Error post-OT summary generation: java.nio.file.NoSuchFileException: /mnt/nfs/ete-exstream/ete-SA/output/DEBTMAN/6fa5e473-8d84-4d67-81fa-5fdcbdc7bcad/archive/5f8cca14-0dfc-46f3-8b75-ea518e981221.tmp

java.io.UncheckedIOException: java.nio.file.NoSuchFileException: /mnt/nfs/ete-exstream/ete-SA/output/DEBTMAN/6fa5e473-8d84-4d67-81fa-5fdcbdc7bcad/archive/5f8cca14-0dfc-46f3-8b75-ea518e981221.tmp
	at java.base/java.nio.file.FileTreeIterator.fetchNextIfNeeded(FileTreeIterator.java:87) ~[na:na]
	at java.base/java.nio.file.FileTreeIterator.hasNext(FileTreeIterator.java:103) ~[na:na]
	at java.base/java.util.Iterator.forEachRemaining(Iterator.java:132) ~[na:na]
	at java.base/java.util.Spliterators$IteratorSpliterator.forEachRemaining(Spliterators.java:1845) ~[na:na]
	at java.base/java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:509) ~[na:na]
	at java.base/java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:499) ~[na:na]
	at java.base/java.util.stream.ForEachOps$ForEachOp.evaluateSequential(ForEachOps.java:150) ~[na:na]
	at java.base/java.util.stream.ForEachOps$ForEachOp$OfRef.evaluateSequential(ForEachOps.java:173) ~[na:na]
	at java.base/java.util.stream.AbstractPipeline.evaluate(AbstractPipeline.java:234) ~[na:na]
	at java.base/java.util.stream.ReferencePipeline.forEach(ReferencePipeline.java:596) ~[na:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.buildDetailedProcessedFiles(KafkaListenerService.java:650) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.processAfterOT(KafkaListenerService.java:337) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.lambda$onKafkaMessage$2(KafkaListenerService.java:205) ~[classes!/:na]
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539) ~[na:na]
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264) ~[na:na]
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136) ~[na:na]
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635) ~[na:na]
	at java.base/java.lang.Thread.run(Thread.java:840) ~[na:na]
Caused by: java.nio.file.NoSuchFileException: /mnt/nfs/ete-exstream/ete-SA/output/DEBTMAN/6fa5e473-8d84-4d67-81fa-5fdcbdc7bcad/archive/5f8cca14-0dfc-46f3-8b75-ea518e981221.tmp
	at java.base/sun.nio.fs.UnixException.translateToIOException(UnixException.java:92) ~[na:na]
	at java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:106) ~[na:na]
	at java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:111) ~[na:na]
	at java.base/sun.nio.fs.UnixFileAttributeViews$Basic.readAttributes(UnixFileAttributeViews.java:55) ~[na:na]
	at java.base/sun.nio.fs.UnixFileSystemProvider.readAttributes(UnixFileSystemProvider.java:148) ~[na:na]
	at java.base/sun.nio.fs.LinuxFileSystemProvider.readAttributes(LinuxFileSystemProvider.java:99) ~[na:na]
	at java.base/java.nio.file.Files.readAttributes(Files.java:1851) ~[na:na]
	at java.base/java.nio.file.FileTreeWalker.getAttributes(FileTreeWalker.java:220) ~[na:na]
	at java.base/java.nio.file.FileTreeWalker.visit(FileTreeWalker.java:277) ~[na:na]
	at java.base/java.nio.file.FileTreeWalker.next(FileTreeWalker.java:374) ~[na:na]
	at java.base/java.nio.file.FileTreeIterator.fetchNextIfNeeded(FileTreeIterator.java:83) ~[na:na]
	... 17 common frames omitted
