<?php

define("URLGET", 'http://localhost/tppro/phpClient/service.php');
define("URL_MAKE_INDEX", 'http://localhost/tppro/phpClient/indexService.php');

class main extends CI_Controller {
	
	function __construct(){
		parent::__construct();
		
		$this->load->helper ( 'form' );
		$this->load->helper ( 'url' );
		
		$this->load->library('session');
        $this->load->library('curl');  
		
		$this->load->model("User");
		$this->load->model("User_partitions");
   }
	
	function index() {

        if(!$this->session->userdata('logged_in')) 
          $this->login();
        else{
          $this->load->view("main", array('user_id'=> $this->session->userdata('user_id'), 'query'=>''));
        }  
	}
	
    function add_to_index() {

        if(!$this->session->userdata('logged_in')) 
          $this->login();
        else{
          $this->load->view("add_to_index", array('user_id'=> $this->session->userdata('user_id'), 'query'=>''));
        }  
	}
    
    function make_index($user_id) {

      $directory = "../frontend/uploads/";
    
      $parts=$this->User_partitions->get($user_id);
      $files = glob($directory . $user_id."*.txt");
      $documents = array();
	  
	  foreach($files as $file){
		$documents[]=$this->parse($file);
		//unlink($file);
  	  }
      
      //TODO harcodeado para q agarre solo una particion
      $part=$parts[0]->partition_id;
      
      //1: partition id
      $get=array('documents'=>$documents, 'partitions'=>$part);
	  $res=$this->curl->simple_get(URL_MAKE_INDEX,$get);
      
      //echo '<pre>'; print_r($res); echo '</pre>';
	}
    
    function parse($file=null){
      if (file_exists($file)) {
        $fp = fopen($file, "r");
        $content = fread($fp, filesize($file));
        fclose($fp);
      }
      $content=json_decode($content, true);

      return $content;
    }
    
	function login(){
		
		if(isset($_POST['username'])&& isset($_POST['pass']) )	{	
			
			$user=$this->User->getUserByUsername($_POST['username']);
			
			if(isset($user->name))
			{	
				if($_POST['pass']==$user->password)
				{
					$this->session->set_userdata('logged_in',TRUE);
					$this->session->set_userdata('user_id',$user->id);
                    $this->session->set_userdata('is_admin',$user->is_admin);
					
					redirect(base_url().'/index.php/main');
				}
			}
		}
		else if($this->session->userdata('logged_in') )
		{
			redirect(base_url().'/index.php/main');
		}
		$this->load->view("login");
	}
    
    function search(){
    
        if($_POST){
          $parts=$this->User_partitions->get($_POST['user_id']);
          $query=$_POST['query'];
          
          //TODO harcodeado para q agarre solo una particion
          $part=$parts[0]->partition_id;
          
          $get=array('query'=>'text:'.$query, 'parts'=>$part);
          
 		  $res=$this->curl->simple_get(URLGET,$get);
          
          $result=json_decode($res);
          
          $this->load->view("main", array('user_id'=> $this->session->userdata('user_id'),'query'=>$query, 'result'=>$result));
        }
        else{ 
          $this->login();
        }
    }
    
    function logout(){
		$this->session->unset_userdata('logged_in');
		$this->login();
	}
}

/* End of file main.php */
/* Location: ./system/application/controllers/main.php */
