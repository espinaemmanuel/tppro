<?php $this->load->view('include/header');?>
<?php $this->load->view('menu');?>

<div class="container-fluid">
  
  <br><h2>Users</h2>  
  
  <table class="table">
      <tbody>
          <tr class="list_header">
              <th>User</th>
              <th style="width:150px;">Name</th>
              <th style="width:150px;">Surname</th>
              <th style="width:150px;">Views</th>
          </tr>
          <?php foreach ($users as $user) {?>
            <tr>  
              <th><?= $user->username;?></th>
              <th><?= $user->name;?></th>
              <th><?= $user->surname;?></th>
              <th>  
                <a href="< ?php echo base_url();?>index.php/reports/dinamic_type/< ?=$user->id;?>">Monitor</a> 
                <!--a href="< ?php echo base_url();?>index.php/reports/static_type/< ?=$user->id;?>">Monitor</a><!-->
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
  <script src="<?=baseUrl()?>/assets/js/jquery.min.js"></script>
  <script src="<?=baseUrl()?>/assets/js/bootstrap.min.js"></script>