Chekkirathi, V. (Vinod) good morning.  I am busy with the mobstat service and currently the Exstream process writes out the pdfs for customers requiring mobstat as below.  Now these pdfs should be in the customer portion of the summary file which is fine, but Exstream writes a trigger file that needs to go to mobstat as per screenshot as well.  We need to cater for this 1 trigger file in the summary file as well
 
 
You can add it maybe after the print file or before the print file section in the summary file.  It will always be 1 DropDataFile.trigger so no need for it to be a list
 
Can you please provide the latest Kafka message structure and the update summary file structure as well please?
 
