DEFINE SQRT org.apache.pig.piggybank.evaluation.math.SQRT;
DEFINE POW org.apache.pig.piggybank.evaluation.math.POW;


data =load '$path' USING PigStorage(',') AS (id:chararray, xval:double, yval:double);

distance = FOREACH data GENERATE  id, (SQRT(POW((xval-$x),2) + POW((yval-$y),2)))as dist; 

sorted = order distance BY dist asc;
top    = limit sorted $k;
STORE top INTO '$output';

