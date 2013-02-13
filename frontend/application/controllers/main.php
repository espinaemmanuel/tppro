<?php

define("URLGET", 'http://localhost/tppro/phpClient/service.php');

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
          $this->load->view('include/header');
          $this->load->view("main", array('user_id'=> $this->session->userdata('user_id')));
          $this->load->view('include/footer');
        }  
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
					
					redirect(base_url().'/index.php/main');
				}
			}
		}
		else if($this->session->userdata('logged_in') )
		{
			redirect(base_url().'/index.php/main');
		}
		$this->load->view('include/header');
		$this->load->view("login");
        $this->load->view('include/footer');
	}
    
    function search(){
    
        if($_POST){
          $parts=$this->User_partitions->get($_POST['user_id']);
          
          //TODO harcodeado para q agarre solo una particion
          $part=$parts[0]->user_id;
          
          $get=array('query'=>'text:'.$_POST['query'], 'parts'=>$part);
		  $res=$this->curl->simple_get(URLGET,$get);
        
          echo $res;
          
          $this->load->view('include/header');
          $this->load->view("main", array('user_id'=> $this->session->userdata('user_id')));
          $this->load->view('include/footer');
        }
    }
    
    function logout(){
		$this->session->unset_userdata('logged_in');
		$this->login();
	}
}

/* End of file main.php */
/* Location: ./system/application/controllers/main.php */
