<?php

class users extends CI_Controller {
	
	function __construct(){
		parent::__construct();
		
		$this->load->helper ( 'form' );
		$this->load->helper ( 'url' );
		
		$this->load->library('session');
		
		if(!$this->session->userdata('logged_in')) 
			redirect('main');	
			
		$this->load->model("User");
		$this->load->model("User_partitions");
	}
	
	function index() {
		
		$this->list_users();
	}
	
	function list_users(){
		
		if(!$this->session->userdata('is_admin'))
			redirect('main');
		
		$users=$this->User->getUsers();
		$this->load->view("users/list",array('users' => $users));
	}
	
	function add_user() {

		$errors='';	
		$campos=array();
		
		//TODO validar errores de ingreso de datos
		if ($_POST) {
			if($_POST['name']=='')
				$errors='Se debe completar el nombre';
			else{
				foreach($_POST as $key=>$value){
					$campos[$key]=$value;
				}
				$this->User->insertUser ( $campos );
				redirect('users');
			}
		}
		
		$this->load->view ( 'add_user',array('errors'=>$errors));
	}
	
	function edit_user($id='') {
		
		$errors='';	
		$user=$this->User->getUser($id);
		$campos=array();	
		
		//TODO validar errores de ingreso de datos
		if ($_POST) {

 			if($_POST['name']=='')
				$errors='Se debe completar el nombre';
			else{
				$id=$_POST['id'];
				foreach($_POST as $key=>$value){
					$campos[$key]=$value;
				}
				//unset($campos['id']);
				
				$this->User->updateUser ( $id, $campos );
				redirect('users');
			}
		}
		
		$this->load->view ( 'edit_user',array('user'=>$user,'errors'=>$errors));
	}
	function user_partitions($id='') {
		
		$parts=$this->user_partitions->get($id);
		
		$this->load->view ( 'user_partitions',array('parts'=>$parts));
	}
	
	function delete_user($id='') {
		
		$this->User->deleteUser ( $id );	
		redirect('users');
	}
}

/* End of user users.php */
/* Location: ./system/application/controllers/users.php */