<?php

//define("URL_MONITOR", 'http://192.168.42.130/phpClient/monitorService.php');
class reports extends CI_Controller {
	
	function __construct(){
		parent::__construct();
		
		$this->load->helper ( 'form' );
		$this->load->helper ( 'url' );
		
		$this->load->library('session');
        $this->load->library('curl');  
		
		$this->load->model("User");
		$this->load->model("User_partitions");
   }
	
	function index($id=null) {

        if(!$this->session->userdata('logged_in')) 
          $this->login();
        else{
          $this->dinamic_type($id);
        }  
	}
    
    function static_type ($id){
      
      $partitions=$this->User_partitions->get($id);
      
      $this->load->view("reports/static", array('partitions'=> $partitions));
    }
    
    function dinamic_type ($id){
      
      if(!$this->session->userdata('logged_in')) 
          redirect(base_url().'/index.php/main/login');
      
      $get=array('shard_id'=>1);
	  $res=$this->curl->simple_get(URL_MONITOR,$get);
      $result=json_decode($res, true);
      
      $this->load->view("reports/dinamic", array('nodes'=> $result['nodes'], 'replicas'=>$result['replicas'], 'groupVersion'=>$result['groupVersion']));
    }
}

/* End of file reports.php */
/* Location: ./system/application/controllers/reports.php */
