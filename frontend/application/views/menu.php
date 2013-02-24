<div class="navbar navbar-inverse navbar-fixed-top">
    <div class="navbar-inner">
        <a class="brand" href="<?php echo base_url();?>index.php/main">Home</a>
        <ul class="nav">
            <?php if($this->session->userdata('is_admin')){?>
            <li>
                <a href="<?php echo base_url();?>index.php/users">Users</a>
            </li>
            <?php }?>
            <li>
                <a href="<?php echo base_url();?>index.php/reports/dinamic_type/<?=$this->session->userdata('user_id')?>">Dinamic View</a>
            </li>
            <li>
                <a href="<?php echo base_url();?>index.php/reports/dinamic_type/<?=$this->session->userdata('user_id')?>">Static View</a>
            </li>
            <li>
                <a href="<?php echo base_url();?>index.php/reports/dinamic_type/<?=$this->session->userdata('user_id')?>">Index</a>
            </li>
            <li>
                <a href="<?php echo base_url();?>index.php/main">Search</a>
            </li>
            <li>
                <a class="logOut" href="<?php echo base_url();?>/index.php/main/logout">Logout</a>
            </li>
        </ul>
    </div>
</div>