import json
import boto3
import os
from os import path
import urllib
from urllib.parse import unquote_plus
import uuid
import logging
from PIL import Image
import PIL.Image
from iptcinfo3 import IPTCInfo

s3_client = boto3.client('s3')


def handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        if is_photo_valid(key):
            mini_key = key.rsplit('/', 1).pop()
            thumb_key = get_new_name(key, 'thumb')
            max_key = get_new_name(key, 'm5m')
            big_key = get_new_name(key, 'm4m')
            med_key = get_new_name(key, 'm3m')
            small_key = get_new_name(key, 'm2m')
            dwn_path = '/tmp/{}{}'.format(uuid.uuid4(), mini_key)
            t_up_path = '/tmp/resized-{}'.format(mini_key)
            x_up_path = '/tmp/resized_max-{}'.format(mini_key)
            b_up_path = '/tmp/resized_big-{}'.format(mini_key)
            m_up_path = '/tmp/resized_med-{}'.format(mini_key)
            s_up_path = '/tmp/resized_small-{}'.format(mini_key)

            try:
                process_images(bucket, key, thumb_key, max_key, big_key, med_key, small_key, dwn_path, t_up_path,
                               x_up_path, b_up_path, m_up_path, s_up_path)
            except Exception as e:
                logging.error(str(e))
            finally:
                # Delete temporary files
                delete_tmp_files(dwn_path, t_up_path, x_up_path, b_up_path, m_up_path, s_up_path)


def is_photo_valid(key):
    if key.lower().endswith(('/', '.html', '.svg', '.js', '.css', '.mp4')):
        return False

    key_end = key.rsplit('/', 1).pop()
    if key_end.find('bwt_') == 0:
        return False
    elif key_end.find('thumb_') == 0:
        return False
    elif key_end.find('m5m_') == 0:
        return False
    elif key_end.find('m4m_') == 0:
        return False
    elif key_end.find('m3m_') == 0:
        return False
    elif key_end.find('m2m_') == 0:
        return False
    elif key_end.find('m1m_') == 0:
        return False
    return True


def get_new_name(orig_name, suf):
    orig_name = unquote_plus(orig_name)
    if '/' in orig_name:
        folders = orig_name.rsplit('/', 1)
        key_end = folders.pop()
        return folders[0] + '/' + suf + '_' + key_end
    else:
        return suf + '_' + orig_name


def get_iptc(the_image_path):
    iptc_info = IPTCInfo(the_image_path)
    out = {}
    if 'keywords' in iptc_info:
        kws = map(lambda x: fix_encoding(x), iptc_info['keywords'])
        out['keywords'] = list(kws)
    if 'city' in iptc_info:
        out['city'] = fix_encoding(iptc_info['city'])
    if 'country/primary location name' in iptc_info:
        out['country/primary location name'] = fix_encoding(iptc_info['country/primary location name'])
    if 'caption/abstract' in iptc_info:
        out['caption/abstract'] = fix_encoding(iptc_info['caption/abstract'])
    if 'copyright notice' in iptc_info:
        out['copyright notice'] = fix_encoding(iptc_info['copyright notice'])
    if 'writer/editor' in iptc_info:
        out['writer/editor'] = fix_encoding(iptc_info['writer/editor'])
    if 'credit' in iptc_info:
        out['credit'] = fix_encoding(iptc_info['credit'])
    if 'sub-location' in iptc_info:
        out['sub-location'] = fix_encoding(iptc_info['sub-location'])
    if 'by-line' in iptc_info:
        out['by-line'] = fix_encoding(iptc_info['by-line'])
    if 'headline' in iptc_info:
        out['headline'] = fix_encoding(iptc_info['headline'])
    return out


def fix_encoding(text):
    res = text
    out_charset = 'windows-1255'
    inp_charset = 'ascii'
    if isinstance(text, str):
        res = text.encode(out_charset or 'utf8')
    elif isinstance(text, str) and out_charset:
        try:
            res = str(text, encoding=inp_charset).encode(out_charset)
        except (UnicodeEncodeError, UnicodeDecodeError):
            res = str(text, encoding=inp_charset, errors='replace').encode(out_charset)
    elif isinstance(text, (list, tuple)):
        res = type(text)(list(map(fix_encoding, text)))
    return res


def update_iptc(tmp_img, orig_iptc):
    info = IPTCInfo(tmp_img, force=True)
    valids = ['keywords', 'caption/abstract', 'country/primary location name', 'city', 'sub-location',
              'credit', 'copyright notice', 'writer/editor', 'by-line', 'headline']
    for k in orig_iptc:
        if k in valids:
            info[k] = orig_iptc[k]
    print('saving new copy with IPTC tags:', tmp_img)
    info.save_as(tmp_img, {'overwrite': True})


def resize_images(image_path, t_rz_path, x_rz_path, b_rz_path, m_rz_path, s_rz_path):
    with Image.open(image_path) as image:
        if image.mode == 'CMYK':
            image = image.convert('RGB')

        orig_img = image.copy()
        icc_profile = orig_img.info.get('icc_profile')

        # test if jpeg:
        is_jpg = False
        extension = path.splitext(image_path)[1].lower()
        if extension in ['.jpg', '.jpeg']:
            is_jpg = True

        image.thumbnail((1100, 1100), Image.ANTIALIAS)
        if is_jpg:
            image.save(x_rz_path, optimize=True, quality=94, progressive=True, icc_profile=icc_profile)
        else:
            image.save(x_rz_path, optimize=True, quality=94)

        image = orig_img.copy()
        image.thumbnail((900, 900), Image.ANTIALIAS)
        if is_jpg:
            image.save(b_rz_path, optimize=True, quality=94, progressive=True, icc_profile=icc_profile)
        else:
            image.save(b_rz_path, optimize=True, quality=94)

        image = orig_img.copy()
        image.thumbnail((700, 700), Image.ANTIALIAS)
        if is_jpg:
            image.save(m_rz_path, optimize=True, quality=94, progressive=True, icc_profile=icc_profile)
        else:
            image.save(m_rz_path, optimize=True, quality=94)

        image = orig_img.copy()
        image.thumbnail((500, 500), Image.ANTIALIAS)
        if is_jpg:
            image.save(s_rz_path, optimize=True, quality=94, progressive=True, icc_profile=icc_profile)
        else:
            image.save(s_rz_path, optimize=True, quality=94)

        image = orig_img.copy()
        image.thumbnail((220, 220), Image.ANTIALIAS)
        image.save(t_rz_path, optimize=True)

        # Adding existing IPTC tags to jpg files
        if is_jpg:
            orig_iptc = get_iptc(image_path)
            if len(orig_iptc) > 1 or (len(orig_iptc) == 1 and len(orig_iptc['keywords']) > 0):
                update_iptc(x_rz_path, orig_iptc)
                update_iptc(b_rz_path, orig_iptc)
                update_iptc(m_rz_path, orig_iptc)
                update_iptc(s_rz_path, orig_iptc)


def process_images(bucket, key, thumb_key, max_key, big_key, med_key, s_key, dwn_path, t_up, x_up, b_up, m_up, s_up):
    fixed_key = unquote_plus(key)
    e_args = {'ContentType': 'image/jpeg', 'ACL': 'public-read', 'Expires': 'Mon, 02 May 2022 08:16:32 GMT',
              'CacheControl': 'max-age=15780000'}
    s3_client.download_file(bucket, fixed_key, dwn_path)
    resize_images(dwn_path, t_up, x_up, b_up, m_up, s_up)
    s3_client.upload_file(t_up, bucket, thumb_key, ExtraArgs=e_args)
    s3_client.upload_file(x_up, bucket, max_key, ExtraArgs=e_args)
    s3_client.upload_file(b_up, bucket, big_key, ExtraArgs=e_args)
    s3_client.upload_file(m_up, bucket, med_key, ExtraArgs=e_args)
    s3_client.upload_file(s_up, bucket, s_key, ExtraArgs=e_args)


def delete_tmp_files(dwn_path, t_up, x_up, b_up, m_up, s_up):
    try:
        os.remove(dwn_path)
    except OSError:
        pass

    try:
        os.remove(t_up)
    except OSError:
        pass

    try:
        os.remove(x_up)
    except OSError:
        pass

    try:
        os.remove(b_up)
    except OSError:
        pass

    try:
        os.remove(m_up)
    except OSError:
        pass

    try:
        os.remove(s_up)
    except OSError:
        pass


