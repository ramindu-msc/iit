friends = LOAD '/opt/friends.txt' USING PigStorage(',');
DUMP friends;
