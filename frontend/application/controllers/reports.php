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
        $mirrors=$this->curl->simple_get(URLGET,$parts);
      }
      */
      
      $node1 = (object) array('desc'=>'Descr node 1', 'status'=>'active');
      $node2 = (object) array('desc'=>'Descr node 2', 'status'=>'inactive');
      $node3 = (object) array('desc'=>'Descr node 3', 'status'=>'activating');
        
      $mirrors["1"]= array($node1,  $node2, $node1, $node2, $node3);
      $mirrors["2"]= array($node2, $node2, $node1, $node2, $node1 );
      $mirrors["4"]= array($node1,  $node2, $node1, $node2, $node1 );
      $mirrors["10"]=array($node2, $node1,  $node1, $node3,  $node1 );
      
      //Count the mirrors of the first element of mirrors array
      /*$total_mirrors=count(reset($mirrors));
      
      for ($j=0; $j<$total_mirrors; $j++) {
        for($i=0; $i<count($parts); $i++){
          $mirror=$mirrors[$parts[$i]][$j];
          $class=($mirror ? 'active' : 'inactive');
          $mirrors[$parts[$i]][$j]=$class;
        }
      }*/
      /*echo '<pre>'; print_r($mirrors);echo '</pre>';*/
      $this->load->view("reports/dinamic", array('partitions'=> $parts, 'mirrors'=>$mirrors));
    }
    
    function getStatus(){
      
    }
    
}

/* End of file reports.php */
/* Location: ./system/application/controllers/reports.php */
