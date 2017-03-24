<H1>Parallel Double Greedy Submodular Maximization</H1>

This repository contains the code for the parallel algorithms presented in our <a href="paper/pan-etal-nips2015.pdf">NIPS 2014 paper</a> for unconstrained maximization of non-monotone submodular functions.
We adopt a database-transactional view of the serial double greedy algorithm which provides a provably optimal 1/2 approximation.
Each set of operations on an element is treated as a transaction to be executed in parallel with other transactions.

Our first parallel algorithm, CF-2g (coordination-free double greedy) simply executes the transactions in parallel without coordination between processors / transactions.
CF-2g has better scalability but suffers from possibly worse approximation.

On the other hand, CC-2g (concurrency control double greedy) ensures coordinates transactions by maintaining upper and lower bounds on the running output of the algorithm.
This ensures serializability, that is, the output of CC-2g is equivalent to some order of processing elements by serial double greedy, and hence maintains the optimal 1/2 approximation.
However, the greater need for coordination results in potentially poorer scalability.

<H3>Running examples without compilation</H3>
As a demonstration, <a href=src/main/scala/Examples.scala>Examples.scala</a> implements both max graph cut and set cover.
Data for the examples are provided in the <a href=data/>data</a> directory, sourced from <a href="http://law.di.unimi.it/datasets.php">Laboratory for Web Algorithms</a>.


To run max graph cut, use the command line arguments to Examples 
<pre>java -cp bin/submodmax.jar Examples inputfile=&lt;filename&gt; maxNThreads=&lt;maximum number of threads&gt;</pre>
For example,
<pre>java -cp bin/submodmax.jar Examples inputfile=data/eswiki-2013_rand_symm maxNThreads=4</pre>

<H3>Preprocessing graph data for examples</H3>
Graph data downloaded from <a href="http://law.di.unimi.it/datasets.php">Laboratory for Web Algorithms</a> can be pre-processed (randomizing order and symmetrizing graph) for use by running <a href=src/main/scala/Preprocess.scala>Preprocess.scala</a> with the command line argument
<pre>java -cp bin/submodmax.jar PreprocessGraph inputfile=&lt;filename&gt;</pre>
For example,
<pre>java -cp bin/submodmax.jar PreprocessGraph inputfile=data/eswiki-2013</pre>


<H3>Running examples using SBT</H3>

If you want to modify the code and run the examples again you can either recompile using bin/update_bin or using the sbt launch scripts as below:

To run max graph cut, use the command line arguments to Examples (this will recompile the code if necessary)
<pre>sbt/sbt "run-main Examples inputfile=&lt;filename&gt; maxNThreads=&lt;maximum number of threads&gt;"</pre>
For example,
<pre>sbt/sbt "run-main Examples inputfile=data/eswiki-2013_rand_symm maxNThreads=4"</pre>

<H3>Preprocessing graph data for examples</H3>
Graph data downloaded from <a href="http://law.di.unimi.it/datasets.php">Laboratory for Web Algorithms</a> can be pre-processed (randomizing order and symmetrizing graph) for use by running <a href=src/main/scala/Preprocess.scala>Preprocess.scala</a> with the command line argument
<pre>sbt/sbt "run-main PreprocessGraph inputfile=&lt;filename&gt;"</pre>
For example,
<pre>sbt/sbt "run-main PreprocessGraph inputfile=data/eswiki-2013"</pre>

