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

//$data->response=  getStatus($data->request_vars['shard_id']);
$data->response= getStatus();
$data->response= json_encode($data->response);
RestUtils::sendResponse(200, $data->response, 'application/json');

function getPort($address){
  if(strpos($address, '_')>-1)
    return substr($address, strpos($address, '_')+1);
  return substr($address, strpos($address, ':')+1);
}

function getStatus(){
  try{
  
      $socket = new TSocket ( '192.168.42.128', 9000 );
      $transport = new TBufferedTransport ( $socket, 1024, 1024 );
      $protocol = new TBinaryProtocol ( $transport );
    
      $client = new MonitorClient($protocol);

      $transport->open ();
      
      $nodes=array();
      $replicas=array();
      foreach ($client->getNodes() as $node){
        if($node->type==0)
          $nodes[]=  "'".getPort($node->url)."'";
          //$replicas[$node->url]=array();
      }      

      foreach ($client->getReplicas() as $replica){
        $replicas["'".getPort($replica->nodeUrl)."'"][]=$replica;
      }
      
      //echo "<pre>Replicas procesadas:<br>------<br><br>"; print_r($replicas); echo "</pre>";
      //echo "<pre>Nodes:<br>------<br><br>"; print_r($nodes); echo "</pre>";
      //echo "<pre>Replicas:<br>---------<br><br>"; print_r($client->getReplicas()); echo "</pre>";
      //echo "<pre>GroupVersion:<br>---------<br><br>"; print_r($client->getGroupVersion()); echo "</pre>";
      //exit;
      
      $results = array('nodes'=>$nodes, 'replicas'=>$replicas, 'groupVersion'=>$client->getGroupVersion());
	  $transport->close();
      
      return $results;
      
      } catch ( TException $tx ) {
          print 'TException: ' . $tx->getMessage () . "\n";
      }
}

/*
Nodes:
------

Array
(
    [0] => tppro\Node Object
        (
            [url] => 127.0.1.1_8003
            [type] => 0
        )

    [1] => tppro\Node Object
        (
            [url] => 127.0.1.1_8001
            [type] => 0
        )

)

Replicas:
---------

Array
(
    [0] => tppro\NodePartition Object
        (
            [groupId] => 1
            [partitionId] => 1
            [nodeUrl] => 192.168.42.128:8001
            [status] => READY
        )

    [1] => tppro\NodePartition Object
        (
            [groupId] => 1
            [partitionId] => 1
            [nodeUrl] => 192.168.42.128:8003
            [status] => READY
        )

)
 */

?>