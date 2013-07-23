<?php
namespace tppro;

error_reporting ( E_ALL );

require_once __DIR__ . '/lib/Thrift/ClassLoader/ThriftClassLoader.php';
require_once __DIR__ . '/REST/RestUtils.php';
require_once __DIR__ . '/config/config.php';

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
$data = RestUtils::processRequest("post");

$data->response=  index($data->request_vars['documents'], $data->request_vars['shard_id']);
$data->response= json_encode($data->response);
RestUtils::sendResponse(200, $data->response, 'application/json');

function index($doc_array=null, $shard_id=0){
  try{
      $documents=array();
      foreach ($doc_array as $content){
         $doc=new Document();
         $doc->fields=$content;
         $documents[]=$doc;
      }
  
      $socket = new TSocket ( ENDPOINT_BROKER, BROKERPORT  );
      $transport = new TBufferedTransport ( $socket, 1024, 1024 );
      $protocol = new TBinaryProtocol ( $transport );
	 
      $client = new IndexBrokerClient($protocol);

      $transport->open ();
      
      //error_log("---Documents: ".print_r($documents, true)); //exit;
      $client->index($shard_id, $documents);
    
      $transport->flush();
      
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