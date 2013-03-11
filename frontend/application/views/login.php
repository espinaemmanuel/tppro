<?php $this->load->view('include/header');?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
<title>Login</title>
</head>

<body>
<h2>Ingrese su usuario y contrase&ntilde;a</h2>
<form action="main/login" method="post">
	<input name="username" placeholder="User Name" required focus type="text" value=""/><br/><br/>
    </label><input name="pass" placeholder="Password" required type="password" value=""/><br/><br/>
    <input class="btn btn-primary btn-large" type="submit" value="Aceptar" name="" class="button"/>
</form>
</body>
</html>
<?php $this->load->view('include/footer');?>