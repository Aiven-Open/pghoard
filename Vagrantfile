# -*- mode: ruby -*-
# vi: set ft=ruby :

unless system("sudo -n true")
    puts('Sudo is required, do a sudo true')
    exit
end

Vagrant.configure("2") do |config|
    config.vm.box = "generic/ubuntu2004"
    # 3.3.4 is broken for libvirt
    config.vm.box_version = "3.3.2"
    config.vm.synced_folder ".", "/vagrant"

    $script = <<-SCRIPT
        ssh-keyscan localhost >> ~/.ssh/known_hosts
        ssh-keygen -N '' -f ~/.ssh/id_rsa
    SCRIPT
    config.vm.provision "shell", inline: $script, privileged: false

    $script = <<-SCRIPT
        echo "deb http://apt.postgresql.org/pub/repos/apt/ focal-pgdg main" > /etc/apt/sources.list.d/pgdg.list
        wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
        add-apt-repository -y ppa:deadsnakes/ppa

        apt-get update
        apt-get install -y build-essential libsnappy-dev

        username="$(< /dev/urandom tr -dc a-z | head -c${1:-32};echo;)"
        password=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c${1:-32};echo;)
        useradd -m -U $username
        echo "$username:$password" > /home/vagrant/pghoard-test-sftp-user
        echo "$username:$password" | chpasswd

        mkdir -p /home/$username/.ssh
        cat /home/vagrant/.ssh/id_rsa.pub >> /home/$username/.ssh/authorized_keys
        chown -R $username: /home/$username/.ssh
        chmod -R go-rwx /home/$username/.ssh

        sed -i 's/PasswordAuthentication no/PasswordAuthentication yes/g' /etc/ssh/sshd_config

        # later versions have the Port 22 config disabled (cos its the default), so need to
        # explicitly enable it to avoid ssh only using port 23.
        echo "Port 22" >> /etc/ssh/sshd_config

        # this is for sftp testing
        echo "Port 23" >> /etc/ssh/sshd_config
        echo "Match LocalPort 22" >> /etc/ssh/sshd_config
        echo "	DenyUsers $username" >> /etc/ssh/sshd_config
        systemctl reload ssh
    SCRIPT
    config.vm.provision "shell", inline: $script, privileged: true

    config.vm.define "postgres9" do |config|
        config.vm.hostname = "postgres9.test"

        $script = <<-SCRIPT
            DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql-9.5 postgresql-server-dev-9.5 python3 python3-dev python3-venv python3.6 python3.6-dev python3.6-venv
        SCRIPT
        config.vm.provision "shell", inline: $script, privileged: true

        $script = <<-SCRIPT
            versions=(3.6)
            python3.6 -m venv venv3.6
            source ~/venv3.6/bin/activate
            pip install --upgrade pip
            pip install -r /vagrant/requirements.txt
            pip install -r /vagrant/requirements.dev.txt

            echo "source ~/venv3.6/bin/activate" >> ~/.bashrc
        SCRIPT
        config.vm.provision "shell", inline: $script, privileged: false
    end

    config.vm.define "postgres10" do |config|
        config.vm.hostname = "postgres10.test"

        $script = <<-SCRIPT
            DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql-10 postgresql-server-dev-10 python3.7 python3.7-dev python3.7-venv
        SCRIPT
        config.vm.provision "shell", inline: $script, privileged: true

        $script = <<-SCRIPT
            python3.7 -m venv venv3.7
            source ~/venv3.7/bin/activate
            pip install --upgrade pip
            pip install -r /vagrant/requirements.txt
            pip install -r /vagrant/requirements.dev.txt

            echo "source ~/venv3.7/bin/activate" >> ~/.bashrc

        SCRIPT
        config.vm.provision "shell", inline: $script, privileged: false
    end
end
