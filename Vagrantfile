# -*- mode: ruby -*-
# vi: set ft=ruby :

if ENV['VAGRANT_DEFAULT_PROVIDER'] == "libvirt" and (ARGV[0] == "up" or ARGV[0] == "destroy")
    unless system("sudo -n true 2> /dev/null")
        puts('Sudo is required, do a sudo true')
        exit
    end
end

# to be able to modify tests outside of vagrant, we use nfs mount point, for more
# information refer https://www.vagrantup.com/docs/synced-folders/nfs
Vagrant.configure("2") do |config|
    config.vm.box = "generic/ubuntu2204"
    config.vm.synced_folder ".", "/vagrant", type: "nfs", nfs_udp: false

    $script = <<-SCRIPT
        ssh-keyscan localhost >> ~/.ssh/known_hosts
        ssh-keygen -N '' -f ~/.ssh/id_rsa
    SCRIPT
    config.vm.provision "shell", inline: $script, privileged: false

    $script = <<-SCRIPT
        export DEBIAN_FRONTEND="noninteractive"

        # do not disable ipv6, the base vagrant image has disabled this
        sysctl net.ipv6.conf.all.disable_ipv6=0
        sed -i '/net.ipv6.conf.all.disable_ipv6/d' /etc/sysctl.conf

        echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
        wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
        add-apt-repository -y ppa:deadsnakes/ppa

        apt-get update
        apt-get install -y build-essential libsnappy-dev postgresql-common

        # no point creating the default cluster as its not used for tests
        sed -i "s/^#start_conf.*/start_conf='manual'/g" /etc/postgresql-common/createcluster.conf
        sed -i "s/^#create_main_cluster.*/create_main_cluster=false/g" /etc/postgresql-common/createcluster.conf

        apt-get install -y python{3.10,3.11,3.12} python{3.10,3.11,3.12}-dev python{3.10,3.11,3.12}-venv
        apt-get install -y postgresql-{12,13,14,15,16} postgresql-server-dev-{12,13,14,15,16}

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

    $script = <<-SCRIPT
        versions=(3.10 3.11 3.12)
        for version in "${versions[@]}"; do
            python${version} -m venv venv${version}
            source ~/venv${version}/bin/activate
            pip install --upgrade pip
            pip install "/vagrant/."
            pip install --upgrade "/vagrant/.[dev]"
        done
    SCRIPT
    config.vm.provision "shell", inline: $script, privileged: false
end
