# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
	config.vm.box = "ubuntu/xenial64"
    config.vm.network "private_network", type: "dhcp"

    $suscript = <<-SCRIPT
        echo "deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main" > /etc/apt/sources.list.d/pgdg.list
        wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
        sudo add-apt-repository -y ppa:deadsnakes/ppa

        apt-get update
        apt-get install -y build-essential libsnappy-dev
    SCRIPT
    config.vm.provision "shell", inline: $suscript, privileged: true

    config.vm.define "postgres9" do |postgres9|
        postgres9.vm.hostname = "postgres9.test"

        $suscript = <<-SCRIPT
            apt-get install -y postgresql-9.4 postgresql-server-dev-9.4 python3 python3-dev python3-venv python3.6 python3.6-dev python3.6-venv
        SCRIPT
        postgres9.vm.provision "shell", inline: $suscript, privileged: true

        $script = <<-SCRIPT
            versions=(3 3.6)
            for version in "${versions[@]}"; do
                python${version} -m venv venv${version}
                source ~/venv${version}/bin/activate
                pip install --upgrade pip
                pip install astroid==2.0.0 botocore cryptography flake8 httplib2 mock psycopg2 pylint==2.2.2 pytest python-dateutil python-snappy python-systemd requests azure-storage
            done

            echo "source ~/venv3/bin/activate" >> ~/.bashrc
        SCRIPT
        postgres9.vm.provision "shell", inline: $script, privileged: false
    end

    config.vm.define "postgres10" do |postgres10|
        postgres10.vm.hostname = "postgres10.test"

        $suscript = <<-SCRIPT
            apt-get install -y postgresql-10 postgresql-server-dev-10 python3.7 python3.7-dev python3.7-venv
        SCRIPT
        postgres10.vm.provision "shell", inline: $suscript, privileged: true

        $script = <<-SCRIPT
            python3.7 -m venv venv3.7
            source ~/venv3.7/bin/activate
            pip install --upgrade pip
            pip install astroid==2.0.0 botocore cryptography flake8 httplib2 mock psycopg2 pylint==2.2.2 pytest python-dateutil python-snappy python-systemd requests azure-storage

            echo "source ~/venv3.7/bin/activate" >> ~/.bashrc
        SCRIPT
        postgres10.vm.provision "shell", inline: $script, privileged: false
    end
end
