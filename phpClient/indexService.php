<?php
namespace tppro;

error_reporting ( E_ALL );

require_once __DIR__ . '/lib/Thrift/ClassLoader/ThriftClassLoader.php';
require_once __DIR__ . '/REST/RestUtils.php';

use Thrift\ClassLoader\ThriftClassLoader;

$GEN_DIR = realpath ( dirname ( __FILE__ ) ) . '/gen-php';

$loader = new ThriftClassLoader ();
$loader->registerNamespace ( 'Thrift', __DIR__ . '/lib' );
$loader->registerDefinition ( 'shared', $GEN_DIR );
$loader->registerDefinition ( 'tppro', $GEN_DIR );
$loader->register ();

use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TSocket;
use Thrift\Transport\TBufferedTransport;
use Thrift\Exception\TException;

//Service call
$data = RestUtils::processRequest();

$data->response=  index($data->request_vars['user_id'], $data->request_vars['partitions']);
$data->response= json_encode($data->response);
RestUtils::sendResponse(200, $data->response, 'application/json');

//TODO el segundo parametro deberian ser varias particiones pertenecientes al usuario.
function index($user_id=null, $partition_id=1){
  try{
  
      $socket = new TSocket ( 'localhost', 9090 );
      $transport = new TBufferedTransport ( $socket, 1024, 1024 );
      $protocol = new TBinaryProtocol ( $transport );
	  $directory = "../../frontend/uploads/";
    
      $client = new IndexNodeClient($protocol);

      $transport->open ();
      
	  if(!$client->containsPartition($partition_id)){
		$client->createPartition($partition_id);
	  }
	
	  $files = glob($directory . $user_id."*.txt");
      $documents = array();
	  
	  //var_dump($files);
	  foreach($files as $file){
		//echo "Se procesa: $file<br>";
		$documents[]=parse($file);
		unlink($file);
  	  }

  	  foreach ($documents as $document) {
        $client->index($partition_id, array($document));
      }
	  return 1;
    } 
	catch ( TException $tx ) {
	  print 'TException: ' . $tx->getMessage () . "\n";
    }
}

function parse($file=null){
  $doc = new Document();
  
  if (file_exists($file)) {
    $fp = fopen($file, "r");
    $content = fread($fp, filesize($file));
    fclose($fp);
  }
  $doc->fields = json_decode($content, true);
  
  return $doc;
}

?>