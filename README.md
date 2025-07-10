2025-07-10T07:45:00.594Z  INFO 1 --- [           main] .s.b.a.l.ConditionEvaluationReportLogger :

Error starting ApplicationContext. To display the condition evaluation report re-run your application with 'debug' enabled.
2025-07-10T07:45:00.643Z ERROR 1 --- [           main] o.s.b.d.LoggingFailureAnalysisReporter   :

***************************
APPLICATION FAILED TO START
***************************

Description:

The bean 'kafkaTemplate', defined in class path resource [com/nedbank/kafka/filemanage/config/KafkaProducerConfig.class], could not be registered. A bean with that name has already been defined in class path resource [com/nedbank/kafka/filemanage/config/KafkaConfig.class] and overriding is disabled.

Action:

Consider renaming one of the beans or enabling overriding by setting spring.main.allow-bean-definition-overriding=true
