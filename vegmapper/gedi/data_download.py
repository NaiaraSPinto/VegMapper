import requests
import os


def download_from_lpdaac(h5_download_file, out_text_file_name, save_dir, token):
    with open(os.path.join(save_dir, out_text_file_name), 'w') as fw:
        with open(h5_download_file) as fr:
            for url in fr.readlines():
                # Create and submit request and download file
                headers = {'Authorization': 'Bearer ' + token}
                download_file_name = url.split('/')[-1].strip()
                saveName = os.path.join(save_dir, download_file_name)
                with requests.get(url.strip(), verify=False, stream=True, headers=headers) as response:
                    if response.status_code != 200:
                        print(f"{url} not downloaded, check if you are using the correct credentials")
                    else:
                        response.raw.decode_content = True
                        content = response.raw
                        with open(saveName, 'wb') as d:
                            while True:
                                chunk = content.read(16 * 1024)
                                if not chunk:
                                    break
                                d.write(chunk)
                        print('Downloaded file: {}'.format(saveName))
                        fw.writelines(f'{download_file_name}\n')
    return out_text_file_name


def delete_local_files(text_file, dir):
    with open(text_file) as fr:
        for file in fr.readlines():
            os.remove(os.path.join(dir, file.strip()))
            print(f'{file} Deleted!')


def divide_download_file(h5_download_file_in, save_dir):
    divided_files = []
    with open(h5_download_file_in) as fr:
        for url in fr.readlines():
            download_file_name = url.split('/')[-1].split('.')[0]
            h5_download_file_out = os.path.join(save_dir, download_file_name + '.download')
            with open(h5_download_file_out, 'w') as fw:
                fw.writelines(f'{url}')
            divided_files.append(h5_download_file_out)
    print(f'Successfully divided {h5_download_file_in} into {len(divided_files)} files located in {save_dir}')
    return divided_files












