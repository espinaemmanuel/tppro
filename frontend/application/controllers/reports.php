<?php

define("URLGET", 'http://localhost/tppro/phpClient/getStatus.php');

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
      
      $parts=$this->User_partitions->getList($id);
      
      /*
      $mirrors=array();
      if($partitions){
        $mirrors=$this->curl->simple_get(URLGET,$get);
      }
      */
      
      $mirrors["1"]=array(true, false, true, false, false);
      $mirrors["2"]=array(false, false, true, false, true);
      $mirrors["4"]=array(true, false, true, false, true);
      $mirrors["10"]=array(false, true, true, true, true);
      
      for ($j=0; $j<count($mirrors['1']); $j++) {
        
        for($i=0; $i<count($parts); $i++){
         
          $mirror=$mirrors[$parts[$i]][$j];
          
          //echo "mirror: $mirror<br>";
          $class=($mirror ? 'active' : 'inactive');
          //echo '"'.$parts[$i].'"'."-$j<br>";
          //echo "class: $class<br>";
          
          $mirrors[$parts[$i]][$j]=$class;
        }
      }
      
      echo '<pre>';
      //print_r($mirrors);
      echo '</pre>';
      
      $this->load->view("reports/dinamic", array('partitions'=> $parts, 'mirrors'=>$mirrors));
    }
    
    
}

/* End of file reports.php */
/* Location: ./system/application/controllers/reports.php */
