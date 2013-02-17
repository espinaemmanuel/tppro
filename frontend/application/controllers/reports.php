<?php

define("URLGET", 'http://localhost/tppro/phpClient/service.php');

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
          $this->static_type($id);
        }  
	}
    
    function static_type ($id){
      
      $partitions=$this->User_partitions->get($id);
      
      $this->load->view("reports/static", array('partitions'=> $partitions));
    }
    
    function dinamic_type ($id){
      
      $partitions=$this->User_partitions->get($id);
      $mirrors=array('mirror1', 'mirror2', 'mirror3');
      
      $this->load->view("reports/dinamic", array('partitions'=> $partitions, 'mirrors'=>$mirrors));
    }
}

/* End of file reports.php */
/* Location: ./system/application/controllers/reports.php */
