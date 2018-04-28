This zip file contains code for MapReducer program and pig program for KNN implementation and it has report.pdf .


KNN folder has src inside java folder has mapreduce folders, pig folder has pig implementation.

/****************************************************************************************************************************/

Command to run pig is as follows. Give the input file in path,  pass output folder to output, give x and y values and pass a k value.

pig -param path='/points' -param output='/koutput' -param x=51.8219827 -param y=31.9436557 -param k=7 -x local kknn.pig

************************************************************************************************************************************/


/*************************************************************************************************************************************
 For KNN.java To run the program give the arguments in the following order.

/usr/local/hadoop/hadoop-2.8.1/points /home/krupa/Desktop/outputDir 34.5 45.2 100

<inputfile> <outputDir> <x value> <y vavlue> <k>
*****************************************************************************************************************************************/






 
