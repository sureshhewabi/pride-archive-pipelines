#!/bin/sh

## The LDC sync has to be preformed from a EBI hx-wp-login machine
ssh ebi-cli become pride_adm ssh hx-wp-login snap-release pride 2>&1
