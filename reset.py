from flask import Flask, jsonify
import subprocess

app = Flask(__name__)

compose_file_path = '/home/sergio/Beca_IA_TC3.5/docker-compose.yml'

def execute_docker_compose_down():
    try:
        result = subprocess.run(['docker', 'compose', '-f', compose_file_path,'down'], capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        return e.stderr
    
def execute_docker_rm_volumes():
    try:
        volumes_result = subprocess.run(['docker', 'volume', 'ls', '-q'], capture_output=True, text=True, check=True)
        volumes = volumes_result.stdout.splitlines()

        if not volumes:
            return "No hay volúmenes para eliminar. Ejecutar reset suave (/reset)."

        rm_result = subprocess.run(['docker', 'volume', 'rm', '-f'] + volumes, capture_output=True, text=True, check=True)
        return rm_result.stdout
    except subprocess.CalledProcessError as e:
        return f"Error al ejecutar docker volume rm -f: {e.stderr}"

def execute_docker_compose_up():
    try:
        result = subprocess.run(['docker', 'compose', '-f', compose_file_path, 'up', '-d'], capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        return e.stderr
    
def execute_reboot():
    try:
        result = subprocess.run(['/home/sergio/Beca_IA_TC3.5/my_reboot'], capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        return e.stderr

@app.route('/reset', methods=['GET'])
def reset_docker_compose():
    down_output = execute_docker_compose_down()
    up_output = execute_docker_compose_up()
    return jsonify({
        'down_output': down_output,
        'up_output': up_output
    })

@app.route('/hardreset', methods=['GET'])
def hard_reset_docker_compose():
    down_output = execute_docker_compose_down()
    volume_rm_output = execute_docker_rm_volumes()
    up_output = execute_docker_compose_up()
    return jsonify({
        'down_output': down_output,
        'volume_rm_output': volume_rm_output,
        'up_output': up_output
    })

@app.route('/reboot', methods=['GET'])
def reboot():
    reboot_output = execute_reboot()
    return jsonify({
        'reboot_output': reboot_output
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)