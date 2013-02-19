<?php

class User_partitions extends CI_Model {

 	function __construct()    {
        parent::__construct();
        $this->load->database();
        $this->load->helper ( 'url' );
    }
    
    //Devuelve las colecciones de un usuario
	function get($id)
	{
		$query = $this->db->query('select * from user_partitions where user_id = '.$id);
        return $query->result();
	}
    
    function getList($id)
	{
		$parts=$this->get($id);
        $result=array();
        
        foreach ($parts as $part) 
          $result[]=$part->partition_id;
        
        return $result;
	}
	
	function insertPartition($campos){ 
    }
    
    function deletePartition($id){
  	}
}	