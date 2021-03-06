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
$data = RestUtils::processRequest("get");

$data->response=  search($data->request_vars['query'], $data->request_vars['shard_id']);
$data->response= json_encode($data->response);
RestUtils::sendResponse(200, $data->response, 'application/json');

function search($query='world', $shard_id='0'){
  try{
      //$socket = new TSocket ( '192.168.42.128', 8008 );
      $socket = new TSocket ( ENDPOINT_BROKER, BROKERPORT );
      $transport = new TBufferedTransport ( $socket, 1024, 1024 );
      $protocol = new TBinaryProtocol ( $transport );
    
      $client = new IndexBrokerClient($protocol);

      $transport->open ();
      //echo "query: $query - sid: $shard_id <br>";
      
      //exit;
      
      $results = $client->search($shard_id, $query, 100, 0);

      $transport->close();
      
      return $results;
      
      } catch ( TException $tx ) {
          print 'TException: ' . $tx->getMessage () . "\n";
      }
}

?>