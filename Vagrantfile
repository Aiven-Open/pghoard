# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
	config.vm.box = "ubuntu/xenial64"
    config.vm.hostname = "phgoard.test"
    config.vm.network "private_network", type: "dhcp"

    $suscript = <<-SCRIPT
        echo "deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main" > /etc/apt/sources.list.d/pgdg.list
        wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
        sudo add-apt-repository -y ppa:deadsnakes/ppa

        apt-get update
        apt-get install -y build-essential libsnappy-dev postgresql-9.4 python3 python3-dev python3-venv python3.6 python3.6-dev python3.6-venv python3.7 python3.7-dev python3.7-venv
    SCRIPT
    config.vm.provision "shell", inline: $suscript, privileged: true

    $script = <<-SCRIPT
        versions=(3 3.6 3.7)
        for version in "${versions[@]}"; do
            python${version} -m venv venv${version}
            source ~/venv${version}/bin/activate
            pip install --upgrade pip
            pip install astroid==2.0.0 botocore cryptography flake8 httplib2 mock psycopg2 pylint==2.2.2 pytest python-dateutil python-snappy python-systemd requests azure-storage
        done

        echo "source ~/venv3/bin/activate" >> ~/.bashrc
    SCRIPT
    config.vm.provision "shell", inline: $script, privileged: false
end
