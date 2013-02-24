<?php $this->load->view('include/header');?>
<?php $this->load->view('menu');?>

<div class="container-fluid">
  
  <br><h2>Users</h2>  
  
  <table class="table">
      <tbody>
          <tr class="list_header">
              <th>User</th>
              <th>Name</th>
              <th>Surname</th>
              <th style="width:200px;">Views</th>
          </tr>
          <?php foreach ($users as $user) {?>
            <tr>  
              <th><?= $user->username;?></th>
              <th><?= $user->name;?></th>
              <th><?= $user->surname;?></th>
              <th>  
                <a href="<?php echo base_url();?>index.php/reports/dinamic_type/<?=$user->id;?>">Dinamic</a>&nbsp;|&nbsp; 
                <a href="<?php echo base_url();?>index.php/reports/static_type/<?=$user->id;?>">Static</a>
              </th>
            </tr>  
          <?php }?>
      </tbody>
    </table>
</div>
    
<?php $this->load->view('include/footer');?>

  <!-- Le javascript==================================================-
  ->
  <!-- Placed at the end of the document so the pages load faster -->
  <script src="//ajax.googleapis.com/ajax/libs/jquery/1.8.2/jquery.min.js"></script>
  <script src="//netdna.bootstrapcdn.com/twitter-bootstrap/2.3.0/js/bootstrap.min.js"></script>