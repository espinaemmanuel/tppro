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

//$data->response=  getStatus($data->request_vars['parts']);
$data->response= getStatus(1);
$data->response= json_encode($data->response);
RestUtils::sendResponse(200, $data->response, 'application/json');

function getStatus($parts){
  try{
  
      $socket = new TSocket ( 'localhost', 9090 );
      $transport = new TBufferedTransport ( $socket, 1024, 1024 );
      $protocol = new TBinaryProtocol ( $transport );
    
      $client = new IndexNodeClient($protocol);

      $transport->open ();
      
      //$results = $client->getStatus($parts);
	  
	  //MOCK

		$node1 = (object) array('desc'=>'Descr node 1', 'status'=>'active');
		$node2 = (object) array('desc'=>'Descr node 2', 'status'=>'inactive');
		$node3 = (object) array('desc'=>'Descr node 3', 'status'=>'activating');

		$mirrors["1"]= array($node2, $node2, $node2, $node2, $node3);
		$mirrors["2"]= array($node2, $node2, $node2, $node2, $node1 );
		$mirrors["4"]= array($node2, $node2, $node2, $node2, $node1 );
		$mirrors["10"]=array($node2, $node2, $node2, $node3, $node1 );
		
		$results = $mirrors;

	  //END MOCK	
	
      $transport->close();
      
      return $results;
      
      } catch ( TException $tx ) {
          print 'TException: ' . $tx->getMessage () . "\n";
      }
}

?>