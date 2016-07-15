import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.mllib.feature.{ Word2Vec , Word2VecModel } 



object word2vec_spark{
    
    def tokenize( line : String): Seq[String] = {
	val delimit = "[ 。，“”？：！]+"
	    line.split( delimit ).toSeq
    }

    def main(args: Array[String]){
	val conf = new SparkConf().setAppName("word2vec_spark") 
	val sc = new SparkContext(conf) 
	val local_launch = 1 
	var HDFS ="" // = new String  
	var input_files = "" //new String  
	var SPARK_MASTER ="" // new String  

	var output =""  
	if ( local_launch == 0 ){
	    SPARK_MASTER = "spark://ec2-54-183-167-77.us-west-1.compute.amazonaws.com:7077"
	    HDFS="hdfs://ec2-54-183-167-77.us-west-1.compute.amazonaws.com:9000"
	    input_files = HDFS+"/user/root/workspace/"+args(0)  
	    output = HDFS + "/user/root/workspace/"+args(0)+"/model"
	}
	else{
	    input_files = args(0)  
	    output = "./"+args(0) + "/model"
	}
	
	println(s"loading the files") 
	
	val rdd = sc.wholeTextFiles(input_files)
	println(rdd.count) 

	val text = rdd.map{ case (file, text) => text } 
	val tokens = text.map( doc => tokenize(doc) )
	val word2vec = new Word2Vec()
	val model = word2vec.fit(tokens)  

	val synonyms = model.findSynonyms("科技", 50)
	for((synonym, cosineSimilarity) <- synonyms) {
	    println(s"$synonym $cosineSimilarity")
	}

	// save model
	model.save(sc, output) 

	// load model 
	val model2 =  Word2VecModel.load(sc, output) 
	val synonyms2 = model2.findSynonyms("科技", 50)
	for((synonym, cosineSimilarity) <- synonyms) {
	    println(s"$synonyms2 $cosineSimilarity")
	}

    }
}
 

