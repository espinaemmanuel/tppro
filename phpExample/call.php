<?php
include_once('REST/RestUtils.php');

$data = RestUtils::processRequest();

//print_r($data);

$data->response=  json_encode('hola');


RestUtils::sendResponse(200, $data->response, 'application/json');