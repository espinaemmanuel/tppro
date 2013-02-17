<div class="navbar navbar-inverse navbar-fixed-top">
    <div class="navbar-inner">
        <a class="brand" href="#">Title</a>
        <ul class="nav">
            <li class="active">
                <a href="#">Home</a>
            </li>
            <li>
                <a href="<?php echo base_url();?>index.php/reports/static">Static View</a>
            </li>
            <li>
                <a href="#">Dinamic View</a>
            </li>
        </ul>
    </div>
</div>

<!--? php if($this->session->userdata('user_type')=='admin'){?>
	<a class="addUser" href="< ?php echo base_url();?>/index.php/users/add_user">
		Add user
	</a>
	&nbsp;|&nbsp;
	<a class="listUsers" href="< ?php echo base_url();?>/index.php/users">
		List users
	</a>&nbsp;
< ?php }
else {?>
	<a class="ListCols" href="< ?php echo base_url();?>/index.php/collections">
		My collections
	</a>
	&nbsp;|&nbsp;
	<a class="addCol" href="< ?php echo base_url();?>/index.php/collections/add_collection">
		Add collection
	</a>&nbsp;
< ?php }?>

<br/>

<a class="logOut" style="float:right; position:absolute; left: 700px; top:5px;" href="< ?php echo base_url();?>/index.php/main/logout">
	Loguot
</a>
<br/-->