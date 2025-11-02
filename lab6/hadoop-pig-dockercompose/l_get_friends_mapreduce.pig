friends = LOAD '/user/week_08/friends-mapreduce.txt' USING PigStorage(',');
DUMP friends;
