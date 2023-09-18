from dagster import failure_hook, success_hook, HookContext
from telebot import TeleBot


@failure_hook(required_resource_keys={'tg'})
def telegram_message_on_failure(context: HookContext):
    tg: TeleBot = context.resources.tg
    tg.send_message(314806050, f'failed {context.op.name} operation')


@success_hook(required_resource_keys={'tg'})
def telegram_message_on_success(context: HookContext):
    tg: TeleBot = context.resources.tg
    tg.send_message(314806050, f'success {context.op.name} operation')
