REGISTER $piggybank_jar;

-- Define the XMLLoader with no arguments
DEFINE MyXMLLoader org.apache.pig.piggybank.storage.XMLLoader();

-- Load the XML file using the defined XMLLoader
comments = LOAD '/user/test/stackapps.com/comments/Comments2.xml'
    USING org.apache.pig.piggybank.storage.XMLLoader(
        'Id:int,PostId:int,Score:int,Text:chararray,CreationDate:chararray,UserId:int'
    );
    
-- Load the input text file
lines = LOAD '/user/test/stackapps.com/comments/Comments2.xml' USING PigStorage('\n') AS (line:chararray);
-- Split each line into words
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) AS word;
-- Group by each word
grouped_words = GROUP words BY word;
-- Count the occurrences of each word
word_count = FOREACH grouped_words GENERATE group AS word, COUNT(words) AS count;
DUMP word_count;
STORE word_count INTO '/user/test/top5words';