<div class="navbar navbar-inverse navbar-fixed-top">
    <div class="navbar-inner">
        <a class="brand" href="<?php echo base_url();?>main">Home</a>
        <ul class="nav">
            <?php if($this->session->userdata('is_admin')){?>
              <li>
                  <a href="<?php echo base_url();?>users">Users</a>
              </li>
            <?php } 
            else {?>
              <!--li>
                  <a href="< ?php echo base_url();?>reports/dinamic_type/< ?=$this->session->userdata('user_id')?>">Dinamic View</a>
              </li-->
              <li>
                  <a href="<?php echo base_url();?>reports/dinamic_type/<?=$this->session->userdata('user_id')?>">System Monitor</a>
              </li>
              <li>
                  <a href="<?php echo base_url();?>main/add_to_index">Index</a>
              </li>
              <li>
                  <a href="<?php echo base_url();?>main">Search</a>
              </li>
            <?php } ?>
            <li>
                <a class="logOut" href="<?php echo base_url();?>main/logout">Logout</a>
            </li>
        </ul>
    </div>
</div>