<?php

//define("URL_SEARCH", 'http://192.168.42.130/phpClient/searchService.php');
//define("URL_MAKE_INDEX", 'http://192.168.42.130/phpClient/indexService.php');
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
          $this->load->view("main", array('user_id'=> $this->session->userdata('user_id'), 'title'=>'', 'text'=>'', 'director'=>'','year'=>'', 'genre'=>'', 'operator'=>'AND'));
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
      $files = glob($directory . $user_id."*.txt");
      $documents = array();
	  
	  foreach($files as $file){
        error_log(print_r($file, true));
		$documents[]=$this->parse($file);
		unlink($file);
  	  }
      
      $shard_id=$this->session->userdata('shard_id');
      $get=array('documents'=>$documents, 'shard_id'=>$shard_id);
	  $res=$this->curl->simple_get(URL_MAKE_INDEX,$get);
      //echo '<pre>'; print_r($res); echo '</pre>';exit;
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
                    $this->session->set_userdata('shard_id',$user->shard_id);
                    
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
          $query="";
          
          $title=$text=$director=$year=$genre="";
          $operator=$_POST['operator'];
          
          if($_POST['title']!==''){
            $query.="title: ".$_POST['title']."*";
            $title=$_POST['title'];
            
            if($_POST['text']!==''){
              $query.= " " . $operator . " overview: ". $_POST['text'];
              $text=$_POST['text'];
            }
            else
              $query.= " ";
          }
          else if($_POST['text']!==''){
              $text=$_POST['text'];
              $query.= "overview: " . $_POST['text'] . "* ";
          }
          
          if ($query !== ''){
            
            if($_POST['director']){
              $query.=" AND  director: ". $_POST['director'];
              $director=$_POST['director'];
            }
            if($_POST['year']){
              $query.=" AND  release: [". $_POST['year'] . " TO 2013]";
              $year=$_POST['year'];
            }  
          }
          
          else{
            if($_POST['director']){
              $query.="director: ". $_POST['director'];
              $director=$_POST['director'];
            }
            if($_POST['year']){
              if($_POST['director'])
                $query.=" AND ";
              $query.="release: [" . $_POST['year']. " TO 2013]";
              $year=$_POST['year'];
            }
          }  
          //echo "<br><pre>";print_r($query); echo "</pre>";//exit;
          $shard_id=$this->session->userdata('shard_id');
          $get=array('query'=>$query, 'shard_id'=>$shard_id);
          
 		  $res=$this->curl->simple_get(URL_SEARCH,$get);
          //print_r($res); //exit;
          $result=json_decode($res);//exit;
          //echo "<pre>";print_r($result); echo "</pre>";exit;
          $this->load->view("main", array('user_id'=> $this->session->userdata('user_id'),'title'=>$title, 'text'=>$text, 'director'=>$director,'year'=>$year, 'genre'=>$genre, 'operator'=>$operator, 'result'=>$result));
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
